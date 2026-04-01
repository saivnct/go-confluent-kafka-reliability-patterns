package producer

import (
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
)

const (
	DefaultIdempotencyHeaderKey = "x-idempotency-key"
)

// Message is a producer-friendly abstraction used by this wrapper.
// It intentionally mirrors the common fields used by kafka-go message builders.
type Message struct {
	Topic     string
	Key       []byte
	Value     []byte
	Headers   []kafka.Header
	Time      time.Time
	Partition int32
}

func (m Message) toKafkaMessage(idempotencyHeaderKey string) *kafka.Message {
	topic := m.Topic
	partition := kafka.PartitionAny
	if m.Partition >= 0 {
		partition = m.Partition
	}

	headers := cloneHeaders(m.Headers)
	if idempotencyHeaderKey == "" {
		idempotencyHeaderKey = DefaultIdempotencyHeaderKey
	}
	if !hasHeader(headers, idempotencyHeaderKey) {
		headers = append(headers, kafka.Header{Key: idempotencyHeaderKey, Value: []byte(uuid.NewString())})
	}

	msgTime := m.Time
	if msgTime.IsZero() {
		msgTime = time.Now()
	}

	return &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: partition},
		Key:            m.Key,
		Value:          m.Value,
		Headers:        headers,
		Timestamp:      msgTime,
	}
}

func cloneHeaders(headers []kafka.Header) []kafka.Header {
	if len(headers) == 0 {
		return nil
	}
	cloned := make([]kafka.Header, len(headers))
	copy(cloned, headers)
	return cloned
}

func hasHeader(headers []kafka.Header, key string) bool {
	for _, header := range headers {
		if header.Key == key {
			return true
		}
	}
	return false
}
