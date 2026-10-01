package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// PickupMode selects how an idle worker discovers newly claimable jobs.
type PickupMode string

const (
	// PickupPoll sleeps a fixed 500ms between empty claims.
	PickupPoll PickupMode = "poll"
	// PickupListen blocks on LISTEN job_ready and wakes on NOTIFY.
	PickupListen PickupMode = "listen"
)

const (
	defaultFallbackInterval = 5 * time.Second

	// minListenWait floors the scheduled_at-derived timeout. MIN(scheduled_at)
	// can already be in the past when the earliest row is locked by another
	// worker mid-claim; without a floor that would spin.
	minListenWait = 25 * time.Millisecond

	// claimErrorBackoff is the pause after a failed ClaimJob, and the poll
	// interval used while the listener is disconnected (same as PickupPoll).
	claimErrorBackoff = 500 * time.Millisecond
)

// ParsePickupMode parses ATLAS_PICKUP_MODE. Empty means listen. Unknown
// values are an error rather than a silent default so a typo cannot make a
// benchmark measure the wrong mode.
func ParsePickupMode(s string) (PickupMode, error) {
	switch PickupMode(s) {
	case "", PickupListen:
		return PickupListen, nil
	case PickupPoll:
		return PickupPoll, nil
	default:
		return "", fmt.Errorf("invalid pickup mode %q (want %q or %q)", s, PickupPoll, PickupListen)
	}
}

// nextScheduledSQL returns seconds until the earliest pending job in the given
// queues becomes due, or NULL if there are none. The difference is computed on
// the database clock so worker/DB clock skew cannot shift the wakeup.
const nextScheduledSQL = `
SELECT EXTRACT(EPOCH FROM MIN(scheduled_at) - clock_timestamp())::float8
FROM jobs
WHERE state = 'pending'
  AND queue = ANY($1::text[])`

// listenLoop drains all claimable work, then blocks on LISTEN job_ready until
// a notification for one of this worker's queues, the next scheduled_at, or
// the fallback interval — whichever comes first.
//
// Ordering matters: LISTEN is (re)established before each drain, so a job
// that commits mid-drain still leaves a buffered notification and is not
// stranded until the fallback timeout.
//
// Several workers waking on one NOTIFY is expected; FOR UPDATE SKIP LOCKED in
// ClaimJob already makes the losers come back empty-handed.
func (w *Worker) listenLoop(ctx context.Context) {
	l := newJobListener(w.Pool.Config().ConnConfig.Copy(), w.Logger)
	defer l.close()

	served := make(map[string]struct{}, len(w.Queues))
	for _, q := range w.Queues {
		served[q] = struct{}{}
	}

	for {
		if ctx.Err() != nil {
			return
		}

		l.ensureConnected(ctx)

		if err := w.drain(ctx); err != nil {
			sleepCtx(ctx, claimErrorBackoff)
			continue
		}

		if !l.connected() {
			// Degrade to the old poll cadence until the listener is back.
			sleepCtx(ctx, claimErrorBackoff)
			continue
		}

		l.wait(ctx, w.nextWakeTimeout(ctx), served)
	}
}

// drain claims and runs jobs until none are claimable. Returns the claim
// error, if any, so the caller can back off.
func (w *Worker) drain(ctx context.Context) error {
	for ctx.Err() == nil {
		execID := uuid.New()
		job, err := ClaimJob(ctx, w.Pool, w.Queues, w.ID.String(), execID, w.LeaseSeconds)
		if err != nil {
			if ctx.Err() == nil {
				w.Logger.Error("claim error", "err", err)
			}
			return err
		}
		if job == nil {
			return nil
		}
		w.runJob(ctx, job, execID)
	}
	return nil
}

// nextWakeTimeout bounds the LISTEN wait by the next scheduled_at so delayed
// jobs, retry backoffs and reaper reclaims (scheduled 1s out) are picked up on
// time even though their NOTIFY fired before they were due.
func (w *Worker) nextWakeTimeout(ctx context.Context) time.Duration {
	fallback := w.FallbackInterval
	if fallback <= 0 {
		fallback = defaultFallbackInterval
	}

	var secs *float64
	if err := w.Pool.QueryRow(ctx, nextScheduledSQL, w.Queues).Scan(&secs); err != nil {
		if ctx.Err() == nil {
			w.Logger.Warn("next scheduled_at lookup failed; using fallback interval", "err", err)
		}
		return fallback
	}
	if secs == nil {
		return fallback
	}

	d := time.Duration(*secs * float64(time.Second))
	return min(max(d, minListenWait), fallback)
}

func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
