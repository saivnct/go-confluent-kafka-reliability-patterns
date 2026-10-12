package consumer

import (
	"context"
	"time"
)

type polledAtKey struct{}

// PolledAt returns the time the consume loop received the message that is being handled.
// Retries of the same message keep this time, so time.Since(PolledAt(ctx)) is how long the
// consumer has not polled Kafka. It returns false when ctx does not come from the consume loop.
func PolledAt(ctx context.Context) (time.Time, bool) {
	t, ok := ctx.Value(polledAtKey{}).(time.Time)
	return t, ok
}

// WithPolledAt returns a copy of ctx that carries the poll time t. The consume loop uses it for every
// message; applications can use it in tests of code that reads PolledAt.
func WithPolledAt(ctx context.Context, t time.Time) context.Context {
	return context.WithValue(ctx, polledAtKey{}, t)
}
