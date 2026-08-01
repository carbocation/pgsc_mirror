package app

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/carbocation/pgsc_mirror/internal/config"
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
