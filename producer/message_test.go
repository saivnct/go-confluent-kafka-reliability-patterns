package producer

import (
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
)

func TestMessage_ToKafkaMessage_AddsIdempotencyHeaderWhenMissing(t *testing.T) {
	msg := Message{Topic: "topic-a", Key: []byte("k"), Value: []byte("v")}
	kmsg := msg.toKafkaMessage(DefaultIdempotencyHeaderKey)

	assert.NotNil(t, kmsg)
	assert.NotNil(t, kmsg.TopicPartition.Topic)
	assert.Equal(t, "topic-a", *kmsg.TopicPartition.Topic)
	assert.Equal(t, []byte("k"), kmsg.Key)
	assert.Equal(t, []byte("v"), kmsg.Value)

	found := false
	for _, h := range kmsg.Headers {
		if h.Key == DefaultIdempotencyHeaderKey {
			found = true
			assert.NotEmpty(t, h.Value)
		}
	}
	assert.True(t, found)
}

func TestMessage_ToKafkaMessage_DoesNotOverrideIdempotencyHeader(t *testing.T) {
	msg := Message{
		Topic:   "topic-b",
		Headers: []kafka.Header{{Key: DefaultIdempotencyHeaderKey, Value: []byte("fixed")}},
	}
	kmsg := msg.toKafkaMessage(DefaultIdempotencyHeaderKey)

	headers := map[string][]byte{}
	for _, h := range kmsg.Headers {
		headers[h.Key] = h.Value
	}

	assert.Equal(t, []byte("fixed"), headers[DefaultIdempotencyHeaderKey])
}
