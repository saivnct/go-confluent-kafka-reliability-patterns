package test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/saivnct/go-confluent-kafka-reliability-patterns/admin"
	"github.com/saivnct/go-confluent-kafka-reliability-patterns/consumer"
	kkerror "github.com/saivnct/go-confluent-kafka-reliability-patterns/kkErrors"
	"github.com/saivnct/go-confluent-kafka-reliability-patterns/producer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// flakyDLTWriter fails its first failFirst writes (all writes when failFirst < 0), then writes to Kafka.
type flakyDLTWriter struct {
	failFirst int32
	calls     atomic.Int32
}

func (w *flakyDLTWriter) WriteMessages(ctx context.Context, msgs ...producer.Message) error {
	call := w.calls.Add(1)
	if w.failFirst < 0 || call <= w.failFirst {
		return errors.New("dlt write failed (test)")
	}
	return producer.GetKafkaWriter().WriteMessages(ctx, msgs...)
}

// handledValues records the values passed to the handler, in order.
type handledValues struct {
	mu     sync.Mutex
	values []string
}

func (h *handledValues) add(v string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.values = append(h.values, v)
}

func (h *handledValues) get() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.values...)
}

// startDLTFailureScenario writes messages A and B to a new one-partition topic and consumes them with a handler
// that rejects A (terminal error) and accepts B. It returns the consumer, the topic and the handled values.
func startDLTFailureScenario(t *testing.T, dltWriter consumer.DLTWriter, maxWriteAttempts int) (*consumer.BaseKKConsumer, string, *handledValues) {
	t.Helper()
	kafkaURL := mustKafkaURL(t)
	topic := randomTopic("dlt-failure")
	dltTopic := topic + ".DLT"
	require.NoError(t, admin.CreateKafkaTopics(kafkaURL,
		admin.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1},
		admin.TopicConfig{Topic: dltTopic, NumPartitions: 1, ReplicationFactor: 1},
	))
	t.Cleanup(func() { _ = admin.DeleteKafkaTopics(kafkaURL, topic, dltTopic) })

	c, err := consumer.NewBaseKKConsumer("dlt-failure-consumer", consumer.KafkaGroupConsumerConfig{
		BootstrapServers: strings.Split(kafkaURL, ","),
		Topics:           []string{topic},
		GroupID:          randomGroup("dlt-failure-group"),
		AutoOffsetReset:  "earliest",
	}, consumer.KafkaConsumerRetryPolicy{
		MaxFailure:      1,
		BackoffBase:     10 * time.Millisecond,
		BackoffMax:      10 * time.Millisecond,
		BackoffFactor:   1,
		SleepOnFetchErr: 10 * time.Millisecond,
	}, consumer.KafkaConsumerDLTPolicy{
		Enabled:          true,
		Topic:            dltTopic,
		Writer:           dltWriter,
		MaxWriteAttempts: maxWriteAttempts,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	handled := &handledValues{}
	require.NoError(t, c.StartConsume(func(ctx context.Context, m *kafka.Message) error {
		handled.add(string(m.Value))
		if string(m.Value) == "A" {
			return kkerror.NotRetryable(errors.New("message A is rejected"))
		}
		return nil
	}, nil, nil))

	require.NoError(t, producer.GetKafkaWriter().WriteMessages(context.Background(),
		producer.Message{Topic: topic, Key: []byte("key"), Value: []byte("A")},
		producer.Message{Topic: topic, Key: []byte("key"), Value: []byte("B")},
	))
	return c, topic, handled
}

func committedOffset(t *testing.T, c *consumer.BaseKKConsumer, topic string) kafka.Offset {
	t.Helper()
	committed, err := c.Reader.Committed([]kafka.TopicPartition{{Topic: &topic, Partition: 0}}, 5000)
	require.NoError(t, err)
	require.Len(t, committed, 1)
	return committed[0].Offset
}

// The dead-letter write fails once: it is retried, so A reaches the dead-letter topic and is not skipped.
// The handler sees A only once, then B, and the committed offset covers both.
func Test_BaseKKConsumer_DLTWriteFailsOnce_MessageReachesDLT(t *testing.T) {
	dltWriter := &flakyDLTWriter{failFirst: 1}
	c, topic, handled := startDLTFailureScenario(t, dltWriter, 3)

	require.Eventually(t, func() bool { return len(handled.get()) >= 2 }, 20*time.Second, 50*time.Millisecond)
	require.Eventually(t, func() bool { return committedOffset(t, c, topic) == 2 }, 10*time.Second, 100*time.Millisecond)
	assert.Equal(t, []string{"A", "B"}, handled.get())
	assert.Equal(t, int32(2), dltWriter.calls.Load(), "one failed and one successful dead-letter write")

	dltConsumer, err := consumer.NewKafkaGroupConsumer(consumer.KafkaGroupConsumerConfig{
		BootstrapServers: strings.Split(mustKafkaURL(t), ","),
		Topics:           []string{topic + ".DLT"},
		GroupID:          randomGroup("dlt-reader"),
		AutoOffsetReset:  "earliest",
	})
	require.NoError(t, err)
	defer dltConsumer.Close()

	dltMessage, err := pollMessage(dltConsumer, 10*time.Second)
	require.NoError(t, err)
	assert.Equal(t, []byte("A"), dltMessage.Value)
	assert.Equal(t, "0", headerValue(dltMessage, "x-dlt-original-offset"))
}

// The dead-letter write always fails: the consumer stops with KKDLTWriteFailedErr, B is never handled, and
// nothing is committed, so a restart reads A again.
func Test_BaseKKConsumer_DLTWriteAlwaysFails_ConsumerStops(t *testing.T) {
	dltWriter := &flakyDLTWriter{failFirst: -1}
	c, topic, handled := startDLTFailureScenario(t, dltWriter, 3)

	select {
	case <-c.GetConsumeDoneChan():
	case <-time.After(20 * time.Second):
		t.Fatal("the consumer did not stop")
	}

	assert.ErrorIs(t, c.ConsumeErr(), kkerror.KKDLTWriteFailedErr)
	assert.Equal(t, []string{"A"}, handled.get(), "B is not handled after A could not be written to the dead-letter topic")
	assert.Equal(t, int32(3), dltWriter.calls.Load())
	assert.Less(t, int64(committedOffset(t, c, topic)), int64(0), "nothing is committed")
}
