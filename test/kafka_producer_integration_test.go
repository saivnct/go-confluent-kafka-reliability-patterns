package test

import (
	"context"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/saivnct/kafka-cf-reliablity/conn"
	"github.com/saivnct/kafka-cf-reliablity/consumer"
	"github.com/saivnct/kafka-cf-reliablity/producer"
	"github.com/stretchr/testify/assert"
)

const (
	producerPollTimeout          = 10 * time.Second
	producerNoMessagePollTimeout = 2 * time.Second
	producerAsyncResultTimeout   = 5 * time.Second
)

func Test_KafkaWriter_WriteMessages_BatchSuccess_AddsIdempotencyHeader(t *testing.T) {
	kafkaURL := mustKafkaURL(t)
	topic := randomTopic("writer-batch")

	err := conn.CreateKafkaTopics(kafkaURL, conn.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1})
	assert.NoError(t, err)
	defer func() { _ = conn.DeleteKafkaTopics(kafkaURL, topic) }()

	writer := producer.GetKafkaWriter()
	assert.NotNil(t, writer)

	msgs := []producer.Message{
		{Topic: topic, Key: []byte("k1"), Value: []byte("v1")},
		{Topic: topic, Key: []byte("k2"), Value: []byte("v2")},
	}
	err = writer.WriteMessages(context.Background(), msgs...)
	assert.Nil(t, err)

	groupConsumer, err := consumer.NewKafkaGroupConsumer(consumer.KafkaGroupConsumerConfig{
		BootstrapServers: strings.Split(kafkaURL, ","),
		Topics:           []string{topic},
		GroupID:          randomGroup("writer-batch-group"),
		AutoOffsetReset:  "earliest",
	})
	assert.NoError(t, err)
	defer groupConsumer.Close()

	first, err := pollMessage(groupConsumer, producerPollTimeout)
	assert.NoError(t, err)
	second, err := pollMessage(groupConsumer, producerPollTimeout)
	assert.NoError(t, err)

	assert.NotNil(t, first)
	assert.NotNil(t, second)
	assert.NotEmpty(t, headerValue(first, producer.DefaultIdempotencyHeaderKey))
	assert.NotEmpty(t, headerValue(second, producer.DefaultIdempotencyHeaderKey))

	consumed := map[string]string{
		string(first.Key):  string(first.Value),
		string(second.Key): string(second.Value),
	}
	assert.Equal(t, "v1", consumed["k1"])
	assert.Equal(t, "v2", consumed["k2"])
}

func Test_KafkaWriter_WriteMessages_PartialBatch_ReturnsErrorAndDeliversProducedPrefix(t *testing.T) {
	kafkaURL := mustKafkaURL(t)
	topic := randomTopic("writer-partial")

	err := conn.CreateKafkaTopics(kafkaURL, conn.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1})
	assert.NoError(t, err)
	defer func() { _ = conn.DeleteKafkaTopics(kafkaURL, topic) }()

	writer := producer.GetKafkaWriter()
	assert.NotNil(t, writer)

	err = writer.WriteMessages(context.Background(),
		producer.Message{Topic: topic, Key: []byte("k1"), Value: []byte("v1")},
		producer.Message{Topic: "", Key: []byte("k2"), Value: []byte("v2")},
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "kafka topic is required at index 1")

	groupConsumer, err := consumer.NewKafkaGroupConsumer(consumer.KafkaGroupConsumerConfig{
		BootstrapServers: strings.Split(kafkaURL, ","),
		Topics:           []string{topic},
		GroupID:          randomGroup("writer-partial-group"),
		AutoOffsetReset:  "earliest",
	})
	assert.NoError(t, err)
	defer groupConsumer.Close()

	message, err := pollMessage(groupConsumer, producerPollTimeout)
	assert.NoError(t, err)
	assert.NotNil(t, message)
	assert.Equal(t, []byte("k1"), message.Key)
	assert.Equal(t, []byte("v1"), message.Value)
}

func Test_KafkaWriter_WriteMessagesAsync_SucceedsAndReturnsSingleResult(t *testing.T) {
	kafkaURL := mustKafkaURL(t)
	topic := randomTopic("writer-async")

	err := conn.CreateKafkaTopics(kafkaURL, conn.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1})
	assert.NoError(t, err)
	defer func() { _ = conn.DeleteKafkaTopics(kafkaURL, topic) }()

	writer := producer.GetKafkaWriter()
	assert.NotNil(t, writer)

	msg := producer.Message{Topic: topic, Key: []byte("k1"), Value: []byte("v1")}
	resultCh := writer.WriteMessagesAsync(context.Background(), msg)

	select {
	case asyncErr, ok := <-resultCh:
		assert.True(t, ok)
		assert.NoError(t, asyncErr)
	case <-time.After(producerAsyncResultTimeout):
		t.Fatal("timed out waiting for WriteMessagesAsync result")
	}

	select {
	case _, ok := <-resultCh:
		assert.False(t, ok)
	case <-time.After(producerAsyncResultTimeout):
		t.Fatal("timed out waiting for WriteMessagesAsync channel close")
	}

	groupConsumer, err := consumer.NewKafkaGroupConsumer(consumer.KafkaGroupConsumerConfig{
		BootstrapServers: strings.Split(kafkaURL, ","),
		Topics:           []string{topic},
		GroupID:          randomGroup("writer-async-group"),
		AutoOffsetReset:  "earliest",
	})
	assert.NoError(t, err)
	defer groupConsumer.Close()

	message, err := pollMessage(groupConsumer, producerPollTimeout)
	assert.NoError(t, err)
	assert.NotNil(t, message)
	assert.Equal(t, []byte("k1"), message.Key)
	assert.Equal(t, []byte("v1"), message.Value)
}

func Test_KafkaWriter_WriteMessagesFireAndForget_EventuallyConsumes(t *testing.T) {
	kafkaURL := mustKafkaURL(t)
	topic := randomTopic("writer-ff")

	err := conn.CreateKafkaTopics(kafkaURL, conn.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1})
	assert.NoError(t, err)
	defer func() { _ = conn.DeleteKafkaTopics(kafkaURL, topic) }()

	writer := producer.GetKafkaWriter()
	assert.NotNil(t, writer)

	msg := producer.Message{Topic: topic, Key: []byte("k1"), Value: []byte("v1")}
	err = writer.WriteMessagesFireAndForget(context.Background(), msg)
	assert.NoError(t, err)

	groupConsumer, err := consumer.NewKafkaGroupConsumer(consumer.KafkaGroupConsumerConfig{
		BootstrapServers: strings.Split(kafkaURL, ","),
		Topics:           []string{topic},
		GroupID:          randomGroup("writer-ff-group"),
		AutoOffsetReset:  "earliest",
	})
	assert.NoError(t, err)
	defer groupConsumer.Close()

	message, err := pollMessage(groupConsumer, producerPollTimeout)
	assert.NoError(t, err)
	assert.NotNil(t, message)
	assert.Equal(t, []byte("k1"), message.Key)
	assert.Equal(t, []byte("v1"), message.Value)
}

func Test_KafkaWriter_WriteMessagesFireAndForget_CanceledContext_DoesNotPublish(t *testing.T) {
	kafkaURL := mustKafkaURL(t)
	topic := randomTopic("writer-ff-canceled")
	log.Println("topic:", topic)

	err := conn.CreateKafkaTopics(kafkaURL, conn.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1})
	assert.NoError(t, err)
	defer func() { _ = conn.DeleteKafkaTopics(kafkaURL, topic) }()

	writer := producer.GetKafkaWriter()
	assert.NotNil(t, writer)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	msg := producer.Message{Topic: topic, Key: []byte("k1"), Value: []byte("v1")}
	err = writer.WriteMessagesFireAndForget(ctx, msg)
	assert.ErrorIs(t, err, context.Canceled)

	groupConsumer, err := consumer.NewKafkaGroupConsumer(consumer.KafkaGroupConsumerConfig{
		BootstrapServers: strings.Split(kafkaURL, ","),
		Topics:           []string{topic},
		GroupID:          randomGroup("writer-ff-canceled-group"),
		AutoOffsetReset:  "earliest",
	})
	assert.NoError(t, err)
	defer groupConsumer.Close()

	message, err := pollMessage(groupConsumer, producerNoMessagePollTimeout)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Nil(t, message)
}
