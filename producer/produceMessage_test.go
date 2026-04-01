package producer

import (
	"context"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
)

func TestKafkaWriter_WaitForDeliveries_IgnoresNonMessageEvents(t *testing.T) {
	writer := &KafkaWriter{deliveryTimeout: 20 * time.Millisecond}
	deliveryChan := make(chan kafka.Event, 1)
	deliveryChan <- kafka.NewError(kafka.ErrAllBrokersDown, "simulated", false)

	err := writer.waitForDeliveries(context.Background(), deliveryChan, 1)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected kafka delivery event")
	assert.Contains(t, err.Error(), context.DeadlineExceeded.Error())
}

func TestKafkaWriter_WaitForDeliveries_SucceedsForMessageDeliveryReport(t *testing.T) {
	writer := &KafkaWriter{deliveryTimeout: 50 * time.Millisecond}
	deliveryChan := make(chan kafka.Event, 1)
	topic := "topic-a"
	deliveryChan <- &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0}}

	err := writer.waitForDeliveries(context.Background(), deliveryChan, 1)

	assert.NoError(t, err)
}

func TestKafkaWriter_WriteMessagesAsync_EmitsOneResultThenCloses(t *testing.T) {
	writer := &KafkaWriter{}
	resultCh := writer.WriteMessagesAsync(context.Background(), Message{Topic: "topic-a", Value: []byte("v")})

	select {
	case err, ok := <-resultCh:
		assert.True(t, ok)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "kafka writer is not initialized")
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timed out waiting for first async write result")
	}

	select {
	case _, ok := <-resultCh:
		assert.False(t, ok)
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timed out waiting for async result channel close")
	}
}

func TestKafkaWriter_WriteMessagesFireAndForget_ReturnsErrorWhenWriterIsNil(t *testing.T) {
	var writer *KafkaWriter

	err := writer.WriteMessagesFireAndForget(context.Background(), Message{Topic: "topic-a", Value: []byte("v")})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "kafka writer is not initialized")
}

func TestKafkaWriter_WriteMessagesFireAndForget_ReturnsErrorWhenProducerIsNil(t *testing.T) {
	writer := &KafkaWriter{producer: nil}

	err := writer.WriteMessagesFireAndForget(context.Background())

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "kafka writer is not initialized")
}
