package consumer

import (
	"math"
	"time"
)

// KafkaConsumerRetryPolicy controls retry and backoff when handler errors are retryable.
// MaxFailure = 0 means no retry.
// MaxFailure < 0 means retry forever.
// MaxFailure > 0 means retry up to MaxFailure times.
type KafkaConsumerRetryPolicy struct {
	MaxFailure      int
	BackoffBase     time.Duration
	BackoffMax      time.Duration
	BackoffFactor   float64
	SleepOnFetchErr time.Duration
	PollTimeout     time.Duration
}

func DefaultKafkaConsumerRetryPolicy() KafkaConsumerRetryPolicy {
	return KafkaConsumerRetryPolicy{
		MaxFailure:      5,
		BackoffBase:     500 * time.Millisecond,
		BackoffMax:      10 * time.Second,
		BackoffFactor:   2,
		SleepOnFetchErr: 500 * time.Millisecond,
		PollTimeout:     200 * time.Millisecond,
	}
}

func (p KafkaConsumerRetryPolicy) normalize() KafkaConsumerRetryPolicy {
	pDefault := DefaultKafkaConsumerRetryPolicy()

	if p.BackoffBase <= 0 {
		p.BackoffBase = pDefault.BackoffBase
	}
	if p.BackoffMax <= 0 {
		p.BackoffMax = pDefault.BackoffMax
	}
	if p.BackoffFactor < 1 {
		p.BackoffFactor = pDefault.BackoffFactor
	}
	if p.SleepOnFetchErr <= 0 {
		p.SleepOnFetchErr = pDefault.SleepOnFetchErr
	}
	if p.BackoffMax < p.BackoffBase {
		p.BackoffMax = p.BackoffBase
	}
	if p.PollTimeout <= 0 {
		p.PollTimeout = pDefault.PollTimeout
	}
	return p
}

func (p KafkaConsumerRetryPolicy) backoffForAttempt(attempt int) time.Duration {
	if attempt <= 1 {
		return p.BackoffBase
	}
	next := float64(p.BackoffBase) * math.Pow(p.BackoffFactor, float64(attempt-1))
	if next > float64(p.BackoffMax) {
		return p.BackoffMax
	}
	return time.Duration(next)
}
