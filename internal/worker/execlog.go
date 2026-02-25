package worker

import (
	"context"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yourorg/atlas/internal/domain"
)

// writeExecLogStart inserts the execution_log row before the handler runs.
// Writing it at claim time (not completion time) means a crash during execution
// still leaves an audit trail.
func writeExecLogStart(
	ctx context.Context,
	pool *pgxpool.Pool,
	execID uuid.UUID,
	job *domain.Job,
	workerID uuid.UUID,
	hostname, traceID string,
) error {
	_, err := pool.Exec(ctx, `
		INSERT INTO execution_log
			(id, job_id, worker_id, worker_hostname, handler_name,
			 attempt, trace_id, payload, payload_hash)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		execID, job.ID, workerID, hostname, job.HandlerName,
		job.RetryCount, traceID, job.Payload, job.PayloadHash)
	return err
}

// writeExecLogFinish updates the execution_log row with the final outcome.
// Errors are logged but not propagated — the job state has already been
// updated and the outcome loss is non-fatal.
func writeExecLogFinish(
	ctx context.Context,
	pool *pgxpool.Pool,
	execID uuid.UUID,
	outcome string,
	handlerErr error,
	logger *slog.Logger,
) {
	errMsg := ""
	if handlerErr != nil {
		errMsg = handlerErr.Error()
	}
	_, err := pool.Exec(ctx, `
		UPDATE execution_log
		SET finished_at = NOW(), outcome = $1, error_message = $2
		WHERE id = $3
		  AND finished_at IS NULL`, outcome, errMsg, execID)
	if err != nil {
		logger.Error("failed to write exec log finish",
			"exec_id", execID, "err", err)
	}
}
