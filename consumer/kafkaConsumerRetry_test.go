package consumer

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	kkerror "github.com/saivnct/kafka-cf-reliablity/kkErrors"
	"github.com/saivnct/kafka-cf-reliablity/producer"
	"github.com/stretchr/testify/assert"
)

type mockKafkaCommitter struct {
	commitCount int32
	commitErr   error
}

func (m *mockKafkaCommitter) CommitMessage(msg *kafka.Message) ([]kafka.TopicPartition, error) {
	_ = msg
	atomic.AddInt32(&m.commitCount, 1)
	if m.commitErr != nil {
		return nil, m.commitErr
	}
	return []kafka.TopicPartition{}, nil
}

type mockDLTWriter struct {
	writeCount int32
	writeErr   error
	msgs       []producer.Message
}

func (m *mockDLTWriter) WriteMessages(ctx context.Context, msgs ...producer.Message) error {
	_ = ctx
	atomic.AddInt32(&m.writeCount, 1)
	m.msgs = append(m.msgs, msgs...)
	return m.writeErr
}

func testMessage(topic string, key []byte, value []byte, headers []kafka.Header) *kafka.Message {
	return &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 1, Offset: 100},
		Key:            key,
		Value:          value,
		Headers:        headers,
		Timestamp:      time.Now(),
	}
}

func TestProcessKafkaMessageWithRetry_RetryableThenSuccess_Commits(t *testing.T) {
	committer := &mockKafkaCommitter{}
	message := testMessage("test", []byte("k1"), []byte("v1"), nil)
	attempt := int32(0)

	err := ProcessKafkaMessageWithRetry(context.Background(), committer, message,
		func(ctx context.Context, message *kafka.Message) error {
			_ = ctx
			_ = message
			if atomic.AddInt32(&attempt, 1) < 3 {
				return kkerror.Retryable(errors.New("temporary"))
			}
			return nil
		},
		KafkaConsumerRetryPolicy{MaxFailure: 5, BackoffBase: time.Millisecond, BackoffMax: time.Millisecond, BackoffFactor: 1},
	)

	assert.NoError(t, err)
	assert.Equal(t, int32(3), attempt)
	assert.Equal(t, int32(1), atomic.LoadInt32(&committer.commitCount))
}

func TestProcessKafkaMessageWithRetry_NonRetryable_PublishesToDLT_AndCommits(t *testing.T) {
	committer := &mockKafkaCommitter{}
	dlt := &mockDLTWriter{}
	message := testMessage("orders", []byte("k2"), []byte("v2"), nil)
	attempt := int32(0)

	err := ProcessKafkaMessageWithRetryAndOptions(context.Background(), committer, message,
		func(ctx context.Context, message *kafka.Message) error {
			_ = ctx
			_ = message
			atomic.AddInt32(&attempt, 1)
			return kkerror.NotRetryable(errors.New("fatal"))
		},
		KafkaConsumerRetryPolicy{MaxFailure: 5, BackoffBase: time.Millisecond, BackoffMax: time.Millisecond, BackoffFactor: 1},
		KafkaConsumerDLTPolicy{
			Enabled: true,
			Topic:   "orders.DLT",
			Writer:  dlt,
		},
	)

	assert.NoError(t, err)
	assert.Equal(t, int32(1), attempt)
	assert.Equal(t, int32(1), atomic.LoadInt32(&committer.commitCount))
	assert.Equal(t, int32(1), atomic.LoadInt32(&dlt.writeCount))
	assert.Len(t, dlt.msgs, 1)
	assert.Equal(t, "orders.DLT", dlt.msgs[0].Topic)
	assert.Equal(t, []byte("v2"), dlt.msgs[0].Value)
}

func TestProcessKafkaMessageWithRetry_RetryExhausted_DLTWriteFails_NoCommit(t *testing.T) {
	committer := &mockKafkaCommitter{}
	dlt := &mockDLTWriter{writeErr: errors.New("dlt unavailable")}
	message := testMessage("orders", []byte("k3"), []byte("v3"), nil)

	err := ProcessKafkaMessageWithRetryAndOptions(context.Background(), committer, message,
		func(ctx context.Context, message *kafka.Message) error {
			_ = ctx
			_ = message
			return kkerror.Retryable(errors.New("temporary"))
		},
		KafkaConsumerRetryPolicy{MaxFailure: 2, BackoffBase: time.Millisecond, BackoffMax: time.Millisecond, BackoffFactor: 1},
		KafkaConsumerDLTPolicy{
			Enabled: true,
			Topic:   "orders.DLT",
			Writer:  dlt,
		},
	)

	assert.Error(t, err)
	assert.Equal(t, int32(0), atomic.LoadInt32(&committer.commitCount))
	assert.Equal(t, int32(1), atomic.LoadInt32(&dlt.writeCount))
}

func TestProcessKafkaMessageWithRetry_ContextDeadlineExceededDuringBackoff_DoesNotCommit(t *testing.T) {
	committer := &mockKafkaCommitter{}
	message := testMessage("orders", []byte("k4"), []byte("v4"), nil)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := ProcessKafkaMessageWithRetry(ctx, committer, message,
		func(ctx context.Context, message *kafka.Message) error {
			_ = ctx
			_ = message
			return kkerror.Retryable(errors.New("temporary"))
		},
		KafkaConsumerRetryPolicy{MaxFailure: -1, BackoffBase: 200 * time.Millisecond, BackoffMax: 200 * time.Millisecond, BackoffFactor: 1},
	)

	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Equal(t, int32(0), atomic.LoadInt32(&committer.commitCount))
}
