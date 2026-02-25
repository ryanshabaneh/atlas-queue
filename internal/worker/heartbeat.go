package worker

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// RegisterWorker upserts the worker row so execution_log can reference it
// via its worker_id foreign key. Safe to call on restart — ON CONFLICT
// updates the heartbeat and re-marks the worker active.
func RegisterWorker(ctx context.Context, pool *pgxpool.Pool, w *Worker) error {
	return pool.QueryRow(ctx, `
		INSERT INTO workers (id, hostname, queues)
		VALUES ($1, $2, $3)
		ON CONFLICT (id) DO UPDATE
			SET hostname       = EXCLUDED.hostname,
			    queues         = EXCLUDED.queues,
			    status         = 'active',
			    last_heartbeat = NOW()
		RETURNING id`, w.ID, w.Hostname, w.Queues).Scan(&w.ID)
}

// RunHeartbeat updates last_heartbeat every 5 seconds so the reaper can
// distinguish live workers from crashed ones. Stops cleanly on context
// cancellation. Must be run in a goroutine alongside Start().
func (w *Worker) RunHeartbeat(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, err := w.Pool.Exec(ctx,
				`UPDATE workers SET last_heartbeat = NOW() WHERE id = $1`, w.ID)
			if err != nil {
				w.Logger.Error("heartbeat failed", "err", err)
			}
		}
	}
}
