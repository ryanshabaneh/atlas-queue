package domain

// core data types that the whole application revolves around

import (
	"time"

	"github.com/google/uuid"
)

type JobState string

// tracks where the job is in its lifecycle
const (
	StatePending   JobState = "pending"
	StateRunning   JobState = "running"
	StateCompleted JobState = "completed"
	StateDead      JobState = "dead"
	StateCanceled  JobState = "canceled"
)

type Job struct {
	ID                 uuid.UUID  // unique ID for this job
	Queue              string     // which queue it belongs to — "default", "emails", etc.
	HandlerName        string     // which function to call when this job runs
	Payload            []byte     // input data passed to the handler
	PayloadHash        string     // fingerprint of the payload — detects corruption or changes
	State              JobState   // current state: pending, running, completed, dead, canceled
	Priority           int        // higher = picked up first
	ScheduledAt        time.Time  // don't run this job before this time
	CreatedAt          time.Time  // when the job row was inserted into the database
	UpdatedAt          time.Time  // last time anything on this row changed
	CompletedAt        *time.Time // nullable — only set when the job finishes successfully
	CanceledAt         *time.Time // nullable — set when canceled; also signals the worker to stop
	RetryCount         int        // how many times it's been attempted so far
	MaxRetries         int        // how many times it's allowed to retry before going dead
	IdempotencyKey     string     // unique string to prevent duplicate submissions
	LockedBy           *string    // nullable — which worker currently owns this job
	LockedAt           *time.Time // nullable — when the worker claimed it
	LockExpiresAt      *time.Time // nullable — deadline for the worker to renew; if expired, reaper requeues
	LastError          *string    // nullable — error message from the last failed attempt
	LastErrorAt        *time.Time // nullable — when that error happened
	CurrentExecutionID *uuid.UUID // nullable — unique ID for the current attempt; used to reject stale writes
	StateVersion       int        // increments on every state change; stale writers won't match, so their writes are rejected
}

type Worker struct {
	ID            uuid.UUID // unique ID for this worker
	Hostname      string    // name of the machine the worker is running on
	Queues        []string  // which queues this worker listens to — e.g. ["emails", "payments"]
	LastHeartbeat time.Time // last time the worker sent a pulse to the DB saying "I'm alive"
	Status        string    // "active" or "dead" — reaper sets "dead" if heartbeat is stale > 30s
	RegisteredAt  time.Time // when this worker first started up and registered itself
}

// ExecutionLog is the history of every attempt ever made on a job.
// The jobs table tracks current state. This table tracks every attempt — never changes, only appends.
type ExecutionLog struct {
	ID             uuid.UUID  // unique ID for this attempt database key
	JobID          uuid.UUID  // which job this attempt belongs to
	WorkerID       uuid.UUID  // which worker ran it
	WorkerHostname string     // which machine ran it
	HandlerName    string     // which function was called
	Attempt        int        // attempt number (1st try, 2nd try, etc.)
	StartedAt      time.Time  // when this attempt started
	FinishedAt     *time.Time // nullable — when it finished (nil if still running)
	Outcome        *string    // nullable — "completed", "failed", "canceled", "orphaned"
	ErrorMessage   *string    // nullable — error message if it failed
	TraceID        string     // unique ID per attempt for debugging
	Payload        []byte     // input data stored here so replay works even after job is cleaned up
	PayloadHash    string     // fingerprint to verify payload wasn't corrupted
}
