package registry

import (
	"context"
	"fmt"
)

// Handler is the function signature every job handler must implement.
type Handler func(ctx context.Context, payload []byte) error

// FatalError wraps a handler error that must not be retried.
// Return this to move a job directly to the dead state.
type FatalError struct {
	Cause error
}

func (e *FatalError) Error() string { return e.Cause.Error() }
func (e *FatalError) Unwrap() error { return e.Cause }

// Registry maps handler names to Handler functions.
type Registry struct {
	handlers map[string]Handler
}

func New() *Registry {
	return &Registry{handlers: make(map[string]Handler)}
}

func (r *Registry) Register(name string, h Handler) {
	r.handlers[name] = h
}

func (r *Registry) Lookup(name string) (Handler, error) {
	h, ok := r.handlers[name]
	if !ok {
		return nil, fmt.Errorf("no handler registered for: %q", name)
	}
	return h, nil
}

func (r *Registry) Names() []string {
	names := make([]string, 0, len(r.handlers))
	for n := range r.handlers {
		names = append(names, n)
	}
	return names
}
