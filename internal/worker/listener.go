package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	// jobReadyChannel is fed by the triggers in migration 005. The payload is
	// the queue name of the job that just became claimable.
	jobReadyChannel = "job_ready"

	listenBackoffMin = 500 * time.Millisecond
	listenBackoffMax = 10 * time.Second
)

// jobListener owns a dedicated connection that LISTENs on job_ready.
//
// It is deliberately not taken from the pool: LISTEN is session state, and a
// pooled connection would either be held forever (shrinking the pool) or be
// returned with the subscription still attached. A dedicated connection also
// means a listener failure never affects claim/complete traffic.
//
// jobListener is not safe for concurrent use; it is owned by one poll loop.
type jobListener struct {
	connConfig *pgx.ConnConfig
	logger     *slog.Logger

	conn     *pgx.Conn
	backoff  time.Duration
	nextDial time.Time
}

func newJobListener(connConfig *pgx.ConnConfig, logger *slog.Logger) *jobListener {
	return &jobListener{connConfig: connConfig, logger: logger}
}

// connected reports whether LISTEN is currently active.
func (l *jobListener) connected() bool {
	return l.conn != nil && !l.conn.IsClosed()
}

// ensureConnected (re)establishes the LISTEN connection if it is down and the
// reconnect backoff has elapsed. Failures are logged, never returned: the
// caller falls back to polling while disconnected.
func (l *jobListener) ensureConnected(ctx context.Context) {
	if l.connected() {
		return
	}
	l.close()
	if time.Now().Before(l.nextDial) {
		return
	}

	conn, err := pgx.ConnectConfig(ctx, l.connConfig)
	if err == nil {
		if _, err = conn.Exec(ctx, "LISTEN "+jobReadyChannel); err != nil {
			closeConn(conn)
		}
	}
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		l.scheduleReconnect()
		l.logger.Warn("job listener connect failed; polling until reconnected",
			"err", err, "retry_in", l.backoff)
		return
	}

	l.conn = conn
	l.backoff = 0
	l.logger.Info("job listener connected", "channel", jobReadyChannel)
}

// wait blocks until a job_ready notification for one of queues arrives, the
// timeout elapses, or ctx is canceled. A timeout is a normal wake, not an
// error. Notifications for queues this worker does not serve are skipped
// without resetting the deadline. On a connection error the listener is
// closed and a reconnect is scheduled.
func (l *jobListener) wait(ctx context.Context, timeout time.Duration, queues map[string]struct{}) {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	for {
		n, err := l.conn.WaitForNotification(waitCtx)
		if err != nil {
			if waitCtx.Err() != nil {
				// pgx interrupts the read with a net deadline and keeps the
				// connection usable; ensureConnected handles it if not.
				return
			}
			l.logger.Warn("job listener connection lost; polling until reconnected",
				"err", err)
			l.close()
			l.scheduleReconnect()
			return
		}
		if _, ok := queues[n.Payload]; ok {
			return
		}
	}
}

func (l *jobListener) scheduleReconnect() {
	if l.backoff == 0 {
		l.backoff = listenBackoffMin
	} else {
		l.backoff = min(l.backoff*2, listenBackoffMax)
	}
	l.nextDial = time.Now().Add(l.backoff)
}

// close releases the connection. Uses a fresh context so it still sends a
// clean Terminate when the worker's context is already canceled.
func (l *jobListener) close() {
	if l.conn == nil {
		return
	}
	closeConn(l.conn)
	l.conn = nil
}

func closeConn(conn *pgx.Conn) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = conn.Close(ctx)
}
