package consumer

import (
	"context"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	kkerror "github.com/saivnct/go-confluent-kafka-reliability-patterns/kkErrors"
	"github.com/stretchr/testify/assert"
)

func TestPolledAt_NotSet(t *testing.T) {
	_, ok := PolledAt(context.Background())
	assert.False(t, ok)
}

func TestPolledAt_SameTimeOnEveryRetry(t *testing.T) {
	polled := time.Now().Add(-time.Minute)
	ctx := WithPolledAt(context.Background(), polled)
	committer := &mockKafkaCommitter{}
	seen := []time.Time{}

	err := ProcessKafkaMessageWithRetry(ctx, committer, testMessage("test", nil, []byte("v"), nil),
		func(ctx context.Context, message *kafka.Message) error {
			got, ok := PolledAt(ctx)
			assert.True(t, ok)
			seen = append(seen, got)
			if len(seen) < 3 {
				return kkerror.Retryable(assert.AnError)
			}
			return nil
		},
		KafkaConsumerRetryPolicy{MaxFailure: 5, BackoffBase: time.Millisecond, BackoffMax: time.Millisecond, BackoffFactor: 1},
	)
	assert.NoError(t, err)
	assert.Len(t, seen, 3)
	for _, got := range seen {
		assert.True(t, got.Equal(polled))
	}
}
