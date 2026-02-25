package domain

import (
	"time"

	"github.com/google/uuid"
)

type JobState string

const (
	StatePending   JobState = "pending"
	StateRunning   JobState = "running"
	StateCompleted JobState = "completed"
	StateDead      JobState = "dead"
	StateCanceled  JobState = "canceled"
)

type Job struct {
	ID                 uuid.UUID
	Queue              string
	HandlerName        string
	Payload            []byte
	PayloadHash        string
	State              JobState
	Priority           int
	ScheduledAt        time.Time
	CreatedAt          time.Time
	UpdatedAt          time.Time
	CompletedAt        *time.Time
	CanceledAt         *time.Time
	RetryCount         int
	MaxRetries         int
	IdempotencyKey     string
	LockedBy           *string
	LockedAt           *time.Time
	LockExpiresAt      *time.Time
	LastError          *string
	LastErrorAt        *time.Time
	CurrentExecutionID *uuid.UUID
	StateVersion       int
}

type Worker struct {
	ID            uuid.UUID
	Hostname      string
	Queues        []string
	LastHeartbeat time.Time
	Status        string
	RegisteredAt  time.Time
}

type ExecutionLog struct {
	ID             uuid.UUID
	JobID          uuid.UUID
	WorkerID       uuid.UUID
	WorkerHostname string
	HandlerName    string
	Attempt        int
	StartedAt      time.Time
	FinishedAt     *time.Time
	Outcome        *string
	ErrorMessage   *string
	TraceID        string
	Payload        []byte
	PayloadHash    string
}
