package app

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const (
	ServiceStarted   = "started"
	ServiceSucceeded = "succeeded"
	ServiceFailed    = "failed"
	ServiceStopped   = "stopped"
)

// ServiceEvent is one JSON-serializable lifecycle or maintenance result from
// the long-running service.
type ServiceEvent struct {
	At        time.Time  `json:"at"`
	Operation string     `json:"operation"`
	Status    string     `json:"status"`
	Result    any        `json:"result,omitempty"`
	Error     string     `json:"error,omitempty"`
	NextAt    *time.Time `json:"next_at,omitempty"`
}

// ServiceReporter receives service events. Returning an error stops the
// service, which lets CLI output failures propagate normally.
type ServiceReporter func(ServiceEvent) error

// Run maintains the mirror until ctx is canceled. It synchronizes immediately
// on startup, performs cheap update checks at the configured interval, runs a
// full reconciliation when the durable state says one is due, and samples the
// completed mirror periodically. Operational failures are reported and retried
// instead of terminating the process.
func (a *App) Run(ctx context.Context, report ServiceReporter) error {
	if a.State == nil {
		return errors.New("writable state is not open")
	}
	if report == nil {
		report = func(ServiceEvent) error { return nil }
	}
	if ctx.Err() != nil {
		return nil
	}
	now := a.now().UTC()
	nextSync := now
	lastVerify, verifiedBefore, err := a.State.LastSuccessfulVerification(ctx)
	if err != nil {
		return fmt.Errorf("read verification schedule: %w", err)
	}
	nextVerify := now
	if verifiedBefore {
		nextVerify = lastVerify.Add(a.Config.Service.VerifyInterval.Duration)
	}
	verificationReady := false
	if err := report(ServiceEvent{At: now, Operation: "service", Status: ServiceStarted, NextAt: timePointer(nextSync)}); err != nil {
		return err
	}

	for {
		if ctx.Err() != nil {
			return report(ServiceEvent{At: a.now().UTC(), Operation: "service", Status: ServiceStopped})
		}
		now = a.now().UTC()
		if !now.Before(nextSync) {
			operation, result, err := a.runScheduledSync(ctx, now)
			if ctx.Err() != nil {
				return report(ServiceEvent{At: a.now().UTC(), Operation: "service", Status: ServiceStopped})
			}
			finished := a.now().UTC()
			event := ServiceEvent{At: finished, Operation: operation, Result: result}
			if err != nil {
				event.Status = ServiceFailed
				event.Error = err.Error()
				nextSync = finished.Add(a.Config.Service.ErrorBackoff.Duration)
			} else {
				event.Status = ServiceSucceeded
				nextSync = finished.Add(a.Config.Service.UpdateInterval.Duration)
				verificationReady = true
			}
			event.NextAt = timePointer(nextSync)
			if err := report(event); err != nil {
				return err
			}
			continue
		}
		if verificationReady && !now.Before(nextVerify) {
			result, err := a.runScheduledVerification(ctx)
			if ctx.Err() != nil {
				return report(ServiceEvent{At: a.now().UTC(), Operation: "service", Status: ServiceStopped})
			}
			finished := a.now().UTC()
			event := ServiceEvent{At: finished, Operation: "verify", Result: result}
			if err != nil {
				event.Status = ServiceFailed
				event.Error = err.Error()
				nextVerify = finished.Add(a.Config.Service.ErrorBackoff.Duration)
			} else {
				event.Status = ServiceSucceeded
				nextVerify = finished.Add(a.Config.Service.VerifyInterval.Duration)
			}
			event.NextAt = timePointer(nextVerify)
			if err := report(event); err != nil {
				return err
			}
			continue
		}

		next := nextSync
		if verificationReady && nextVerify.Before(next) {
			next = nextVerify
		}
		wait := next.Sub(a.now().UTC())
		if wait < 0 {
			wait = 0
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return report(ServiceEvent{At: a.now().UTC(), Operation: "service", Status: ServiceStopped})
		case <-timer.C:
		}
	}
}

func (a *App) runScheduledVerification(ctx context.Context) (report VerifyReport, runErr error) {
	runID, err := a.State.BeginRun(ctx, "verify")
	if err != nil {
		return report, err
	}
	defer func() {
		finishErr := a.State.FinishRun(context.Background(), runID, runErr)
		runErr = errors.Join(runErr, finishErr)
	}()
	return a.Verify(ctx, false, 0)
}

func (a *App) runScheduledSync(ctx context.Context, now time.Time) (string, any, error) {
	if _, err := a.restoreMaintenanceCheckpoint(ctx); err != nil {
		return "schedule", nil, fmt.Errorf("restore shared maintenance state: %w", err)
	}
	last, ok, err := a.State.LastSuccessfulReconciliation(ctx)
	if err != nil {
		return "schedule", nil, err
	}
	if !ok || !now.Before(last.Add(a.Config.Service.ReconcileInterval.Duration)) {
		result, err := a.Reconcile(ctx, false)
		return "reconcile", result, err
	}
	result, err := a.Update(ctx, false)
	return "update", result, err
}

func timePointer(t time.Time) *time.Time {
	return &t
}
