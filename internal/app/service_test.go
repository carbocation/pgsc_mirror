package app

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/carbocation/pgsc_mirror/internal/config"
	"github.com/carbocation/pgsc_mirror/internal/model"
	"github.com/carbocation/pgsc_mirror/internal/store"
	localstore "github.com/carbocation/pgsc_mirror/internal/store/local"
)

func serviceTestConfig(root, base string) config.Config {
	cfg := integrationConfig(root, base)
	cfg.Transfer.MaxAttempts = 1
	cfg.Service.UpdateInterval = config.Duration{Duration: time.Hour}
	cfg.Service.ReconcileInterval = config.Duration{Duration: 7 * 24 * time.Hour}
	cfg.Service.VerifyInterval = config.Duration{Duration: time.Hour}
	cfg.Service.ErrorBackoff = config.Duration{Duration: time.Millisecond}
	return cfg
}

func TestServiceReconcilesOnFirstStartThenUsesCheapUpdate(t *testing.T) {
	up := newSyntheticUpstream()
	up.set("PGS000001", harmonizedHeaderFixture, "CC0", true)
	srv := httptest.NewServer(up)
	defer srv.Close()
	a, err := New(context.Background(), serviceTestConfig(t.TempDir(), srv.URL), true)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()

	runUntil := func(want string) []ServiceEvent {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var events []ServiceEvent
		err := a.Run(ctx, func(event ServiceEvent) error {
			events = append(events, event)
			if event.Operation == want && event.Status == ServiceSucceeded {
				cancel()
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return events
	}

	first := runUntil("reconcile")
	if !hasServiceEvent(first, "reconcile", ServiceSucceeded) {
		t.Fatalf("first start did not reconcile: %+v", first)
	}
	second := runUntil("update")
	if !hasServiceEvent(second, "update", ServiceSucceeded) || hasServiceEvent(second, "reconcile", ServiceSucceeded) {
		t.Fatalf("restart did not use cheap update: %+v", second)
	}

	// Once the configured audit interval has elapsed, a restart performs a
	// complete reconciliation instead of only checking the sentinels.
	a.Config.Service.ReconcileInterval = config.Duration{Duration: time.Nanosecond}
	third := runUntil("reconcile")
	if !hasServiceEvent(third, "reconcile", ServiceSucceeded) {
		t.Fatalf("overdue restart did not reconcile: %+v", third)
	}
}

func TestFreshMachineRestoresSharedMaintenanceWithoutSidecarSweep(t *testing.T) {
	up := newSyntheticUpstream()
	up.set("PGS000001", harmonizedHeaderFixture, "CC0", true)
	gate := newSidecarGate(up)
	counter := &pathCounter{handler: gate, paths: make(map[string]int)}
	srv := httptest.NewServer(counter)
	defer srv.Close()

	mirrorRoot := t.TempDir()
	firstState := t.TempDir()
	firstCfg := serviceTestConfig(mirrorRoot, srv.URL)
	firstCfg.State.Path = filepath.Join(firstState, "state.db")
	a, err := New(context.Background(), firstCfg, true)
	if err != nil {
		t.Fatal(err)
	}
	first, err := a.Reconcile(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if gate.count("PGS000001") != 1 {
		t.Fatalf("initial reconciliation sidecar calls=%d, want 1", gate.count("PGS000001"))
	}

	secondState := t.TempDir()
	secondCfg := serviceTestConfig(mirrorRoot, srv.URL)
	secondCfg.State.Path = filepath.Join(secondState, "state.db")
	a2, err := New(context.Background(), secondCfg, true)
	if err != nil {
		t.Fatal(err)
	}
	defer a2.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var events []ServiceEvent
	err = a2.Run(ctx, func(event ServiceEvent) error {
		events = append(events, event)
		if event.Operation == "update" && event.Status == ServiceSucceeded {
			cancel()
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !hasServiceEvent(events, "update", ServiceSucceeded) || hasServiceEvent(events, "reconcile", ServiceSucceeded) {
		t.Fatalf("fresh machine did not use shared maintenance checkpoint: %+v", events)
	}
	if got := gate.count("PGS000001"); got != 1 {
		t.Fatalf("fresh machine repeated checksum sidecar request: got %d total calls, want 1", got)
	}
	if counter.count("/root/pgs_scores_list.txt") != 2 || counter.count("/root/metadata/pgs_all_metadata_scores.csv") != 2 {
		t.Fatalf("fresh machine did not perform exactly one lightweight document check: %+v", counter.paths)
	}
	status, err := a2.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status.State.LatestRelease != first.ReleaseID || status.State.Available != 1 {
		t.Fatalf("fresh local state did not recover canonical release: %+v", status.State)
	}
}

func TestExistingLocalStateBackfillsMissingSharedMaintenanceWithoutAudit(t *testing.T) {
	up := newSyntheticUpstream()
	up.set("PGS000001", harmonizedHeaderFixture, "CC0", true)
	gate := newSidecarGate(up)
	srv := httptest.NewServer(gate)
	defer srv.Close()

	cfg := serviceTestConfig(t.TempDir(), srv.URL)
	a, err := New(context.Background(), cfg, true)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if _, err := a.Reconcile(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	info, err := a.targets[0].Stat(context.Background(), model.MaintenanceKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.targets[0].Delete(context.Background(), model.MaintenanceKey, store.DeleteOptions{GenerationMatch: &info.Generation}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var events []ServiceEvent
	err = a.Run(ctx, func(event ServiceEvent) error {
		events = append(events, event)
		if event.Operation == "update" && event.Status == ServiceSucceeded {
			cancel()
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !hasServiceEvent(events, "update", ServiceSucceeded) || gate.count("PGS000001") != 1 {
		t.Fatalf("checkpoint migration repeated full audit: events=%+v sidecars=%d", events, gate.count("PGS000001"))
	}
	if _, _, err := readMaintenanceCheckpoint(context.Background(), a.targets[0].Store); err != nil {
		t.Fatalf("local state did not backfill shared checkpoint: %v", err)
	}
}

func TestFreshMachinePerformsFullAuditWhenSharedMaintenanceIsOverdue(t *testing.T) {
	up := newSyntheticUpstream()
	up.set("PGS000001", harmonizedHeaderFixture, "CC0", true)
	gate := newSidecarGate(up)
	srv := httptest.NewServer(gate)
	defer srv.Close()

	mirrorRoot := t.TempDir()
	firstCfg := serviceTestConfig(mirrorRoot, srv.URL)
	firstCfg.State.Path = filepath.Join(t.TempDir(), "state.db")
	a, err := New(context.Background(), firstCfg, true)
	if err != nil {
		t.Fatal(err)
	}
	a.now = func() time.Time { return time.Now().UTC().Add(-8 * 24 * time.Hour) }
	if _, err := a.Reconcile(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}

	secondCfg := serviceTestConfig(mirrorRoot, srv.URL)
	secondCfg.State.Path = filepath.Join(t.TempDir(), "state.db")
	a2, err := New(context.Background(), secondCfg, true)
	if err != nil {
		t.Fatal(err)
	}
	defer a2.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var events []ServiceEvent
	err = a2.Run(ctx, func(event ServiceEvent) error {
		events = append(events, event)
		if event.Operation == "reconcile" && event.Status == ServiceSucceeded {
			cancel()
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !hasServiceEvent(events, "reconcile", ServiceSucceeded) {
		t.Fatalf("overdue shared checkpoint suppressed full audit: %+v", events)
	}
	if got := gate.count("PGS000001"); got != 2 {
		t.Fatalf("overdue startup sidecar calls=%d, want 2", got)
	}
}

func TestServiceAdoptsNewerSharedAuditBeforeRepeatingOverdueWork(t *testing.T) {
	up := newSyntheticUpstream()
	up.set("PGS000001", harmonizedHeaderFixture, "CC0", true)
	gate := newSidecarGate(up)
	srv := httptest.NewServer(gate)
	defer srv.Close()

	mirrorRoot := t.TempDir()
	old := time.Now().UTC().Add(-8 * 24 * time.Hour)
	firstCfg := serviceTestConfig(mirrorRoot, srv.URL)
	firstCfg.State.Path = filepath.Join(t.TempDir(), "state.db")
	a, err := New(context.Background(), firstCfg, true)
	if err != nil {
		t.Fatal(err)
	}
	a.now = func() time.Time { return old }
	if _, err := a.Reconcile(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}

	newInstance := func() *App {
		t.Helper()
		cfg := serviceTestConfig(mirrorRoot, srv.URL)
		cfg.State.Path = filepath.Join(t.TempDir(), "state.db")
		app, err := New(context.Background(), cfg, true)
		if err != nil {
			t.Fatal(err)
		}
		if restored, err := app.restoreMaintenanceCheckpoint(context.Background()); err != nil || !restored {
			app.Close()
			t.Fatalf("restore old shared checkpoint: restored=%v err=%v", restored, err)
		}
		return app
	}
	firstContender := newInstance()
	defer firstContender.Close()
	secondContender := newInstance()
	defer secondContender.Close()

	if _, err := firstContender.Reconcile(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	operation, _, err := secondContender.runScheduledSync(context.Background(), time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if operation != "update" {
		t.Fatalf("second contender operation=%q, want update", operation)
	}
	if got := gate.count("PGS000001"); got != 2 {
		t.Fatalf("newer shared audit was not adopted; sidecar calls=%d, want 2", got)
	}
}

func TestFreshMachineFallsBackSafelyFromCorruptSharedMaintenance(t *testing.T) {
	up := newSyntheticUpstream()
	up.set("PGS000001", harmonizedHeaderFixture, "CC0", true)
	gate := newSidecarGate(up)
	srv := httptest.NewServer(gate)
	defer srv.Close()

	mirrorRoot := t.TempDir()
	firstCfg := serviceTestConfig(mirrorRoot, srv.URL)
	firstCfg.State.Path = filepath.Join(t.TempDir(), "state.db")
	a, err := New(context.Background(), firstCfg, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Reconcile(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}

	checkpointStore, err := localstore.New(mirrorRoot)
	if err != nil {
		t.Fatal(err)
	}
	info, err := checkpointStore.Stat(context.Background(), model.MaintenanceKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := checkpointStore.Put(context.Background(), model.MaintenanceKey, strings.NewReader("{"), store.PutOptions{GenerationMatch: &info.Generation, ContentType: "application/json"}); err != nil {
		t.Fatal(err)
	}

	secondCfg := serviceTestConfig(mirrorRoot, srv.URL)
	secondCfg.State.Path = filepath.Join(t.TempDir(), "state.db")
	a2, err := New(context.Background(), secondCfg, true)
	if err != nil {
		t.Fatal(err)
	}
	defer a2.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var events []ServiceEvent
	err = a2.Run(ctx, func(event ServiceEvent) error {
		events = append(events, event)
		if event.Operation == "reconcile" && event.Status == ServiceSucceeded {
			cancel()
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !hasServiceEvent(events, "reconcile", ServiceSucceeded) || gate.count("PGS000001") != 2 {
		t.Fatalf("corrupt checkpoint did not trigger one safe full audit: events=%+v sidecars=%d", events, gate.count("PGS000001"))
	}
	if _, _, err := readMaintenanceCheckpoint(context.Background(), checkpointStore); err != nil {
		t.Fatalf("full audit did not repair corrupt checkpoint: %v", err)
	}
}

func TestServiceRetriesAfterOperationalFailure(t *testing.T) {
	up := newSyntheticUpstream()
	up.set("PGS000001", harmonizedHeaderFixture, "CC0", true)
	gate := newSidecarGate(up)
	gate.block("PGS000001", true)
	srv := httptest.NewServer(gate)
	defer srv.Close()
	a, err := New(context.Background(), serviceTestConfig(t.TempDir(), srv.URL), true)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var events []ServiceEvent
	err = a.Run(ctx, func(event ServiceEvent) error {
		events = append(events, event)
		if event.Operation == "reconcile" && event.Status == ServiceFailed {
			gate.block("PGS000001", false)
		}
		if event.Operation == "reconcile" && event.Status == ServiceSucceeded {
			cancel()
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !hasServiceEvent(events, "reconcile", ServiceFailed) || !hasServiceEvent(events, "reconcile", ServiceSucceeded) {
		t.Fatalf("service did not recover from failure: %+v", events)
	}
}

func TestServiceSchedulesVerificationAfterSynchronization(t *testing.T) {
	up := newSyntheticUpstream()
	up.set("PGS000001", harmonizedHeaderFixture, "CC0", true)
	srv := httptest.NewServer(up)
	defer srv.Close()
	cfg := serviceTestConfig(t.TempDir(), srv.URL)
	cfg.Service.VerifyInterval = config.Duration{Duration: time.Millisecond}
	a, err := New(context.Background(), cfg, true)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var events []ServiceEvent
	err = a.Run(ctx, func(event ServiceEvent) error {
		events = append(events, event)
		if event.Operation == "verify" && event.Status == ServiceSucceeded {
			cancel()
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !hasServiceEvent(events, "reconcile", ServiceSucceeded) || !hasServiceEvent(events, "verify", ServiceSucceeded) {
		t.Fatalf("scheduled verification did not complete: %+v", events)
	}
	lastVerify, ok, err := a.State.LastSuccessfulVerification(context.Background())
	if err != nil || !ok {
		t.Fatalf("verification schedule was not persisted: ok=%v err=%v", ok, err)
	}

	// Simulate a restart after enough downtime to make verification overdue.
	// The service must verify immediately after catching up, rather than
	// postponing verification by a fresh interval on every restart.
	a.Config.Service.VerifyInterval = config.Duration{Duration: time.Hour}
	a.now = func() time.Time { return lastVerify.Add(2 * time.Hour) }
	restartCtx, restartCancel := context.WithTimeout(context.Background(), time.Second)
	defer restartCancel()
	var restartEvents []ServiceEvent
	var fallback *time.Timer
	err = a.Run(restartCtx, func(event ServiceEvent) error {
		restartEvents = append(restartEvents, event)
		if event.Operation == "update" && event.Status == ServiceSucceeded {
			fallback = time.AfterFunc(100*time.Millisecond, restartCancel)
		}
		if event.Operation == "verify" && event.Status == ServiceSucceeded {
			restartCancel()
		}
		return nil
	})
	if fallback != nil {
		fallback.Stop()
	}
	if err != nil {
		t.Fatal(err)
	}
	if !hasServiceEvent(restartEvents, "update", ServiceSucceeded) || !hasServiceEvent(restartEvents, "verify", ServiceSucceeded) {
		t.Fatalf("overdue verification was postponed after restart: %+v", restartEvents)
	}
}

func hasServiceEvent(events []ServiceEvent, operation, status string) bool {
	for _, event := range events {
		if event.Operation == operation && event.Status == status {
			return true
		}
	}
	return false
}
