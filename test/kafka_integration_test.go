package test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/saivnct/go-confluent-kafka-reliability-patterns/admin"
	"github.com/saivnct/go-confluent-kafka-reliability-patterns/consumer"
	kkerror "github.com/saivnct/go-confluent-kafka-reliability-patterns/kkErrors"
	"github.com/saivnct/go-confluent-kafka-reliability-patterns/producer"
	"github.com/stretchr/testify/assert"
)

func Test_CreateAndDeleteKafkaTopics(t *testing.T) {
	kafkaURL := mustKafkaURL(t)
	topic := randomTopic("admin")

	err := admin.CreateKafkaTopics(kafkaURL, admin.TopicConfig{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	})
	assert.NoError(t, err)

	// idempotent create
	err = admin.CreateKafkaTopics(kafkaURL, admin.TopicConfig{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	})
	assert.NoError(t, err)

	err = admin.DeleteKafkaTopics(kafkaURL, topic)
	assert.NoError(t, err)

	// idempotent delete
	err = admin.DeleteKafkaTopics(kafkaURL, topic)
	assert.NoError(t, err)
}

func Test_ProcessKafkaMessageWithRetry_RetryExhausted_SendsDLT(t *testing.T) {
	kafkaURL := mustKafkaURL(t)
	sourceTopic := randomTopic("source")
	dltTopic := sourceTopic + ".DLT"

	err := admin.CreateKafkaTopics(kafkaURL,
		admin.TopicConfig{Topic: sourceTopic, NumPartitions: 1, ReplicationFactor: 1},
		admin.TopicConfig{Topic: dltTopic, NumPartitions: 1, ReplicationFactor: 1},
	)
	assert.NoError(t, err)
	defer func() {
		_ = admin.DeleteKafkaTopics(kafkaURL, sourceTopic, dltTopic)
	}()

	writer := producer.GetKafkaWriter()
	assert.NotNil(t, writer)

	msg := producer.Message{
		Topic:   sourceTopic,
		Key:     []byte("order-1"),
		Value:   []byte("payload-1"),
		Headers: nil,
		Time:    time.Now().UTC(),
	}
	err = writer.WriteMessages(context.Background(), msg)
	assert.Nil(t, err)

	sourceConsumer, err := consumer.NewKafkaGroupConsumer(consumer.KafkaGroupConsumerConfig{
		BootstrapServers: strings.Split(kafkaURL, ","),
		Topics:           []string{sourceTopic},
		GroupID:          randomGroup("source-group"),
		AutoOffsetReset:  "earliest",
	})
	assert.NoError(t, err)
	defer sourceConsumer.Close()

	sourceMessage, err := pollMessage(sourceConsumer, 10*time.Second)
	assert.NoError(t, err)
	assert.NotNil(t, sourceMessage)

	attempts := int32(0)
	err = consumer.ProcessKafkaMessageWithRetryAndOptions(context.Background(), sourceConsumer, sourceMessage,
		func(ctx context.Context, message *kafka.Message) error {
			_ = ctx
			_ = message
			atomic.AddInt32(&attempts, 1)
			return kkerror.Retryable(errors.New("temporary"))
		},
		consumer.KafkaConsumerRetryPolicy{
			MaxFailure:    2,
			BackoffBase:   10 * time.Millisecond,
			BackoffMax:    10 * time.Millisecond,
			BackoffFactor: 1,
		},
		consumer.KafkaConsumerDLTPolicy{
			Enabled: true,
			Topic:   dltTopic,
			Writer:  producer.GetKafkaWriter(),
		},
	)
	assert.NoError(t, err)
	assert.Equal(t, int32(2), attempts)

	dltConsumer, err := consumer.NewKafkaGroupConsumer(consumer.KafkaGroupConsumerConfig{
		BootstrapServers: strings.Split(kafkaURL, ","),
		Topics:           []string{dltTopic},
		GroupID:          randomGroup("dlt-group"),
		AutoOffsetReset:  "earliest",
	})
	assert.NoError(t, err)
	defer dltConsumer.Close()

	dltMessage, err := pollMessage(dltConsumer, 10*time.Second)
	assert.NoError(t, err)
	assert.NotNil(t, dltMessage)
	assert.Equal(t, []byte("payload-1"), dltMessage.Value)
	assert.Equal(t, "2", headerValue(dltMessage, "x-dlt-failure-attempts"))
	assert.Equal(t, sourceTopic, headerValue(dltMessage, "x-dlt-original-topic"))
}

func Test_BaseKKConsumer_StartConsume_WithHooks(t *testing.T) {
	kafkaURL := mustKafkaURL(t)
	topic := randomTopic("base-consumer")

	err := admin.CreateKafkaTopics(kafkaURL, admin.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1})
	assert.NoError(t, err)
	defer func() {
		_ = admin.DeleteKafkaTopics(kafkaURL, topic)
	}()

	baseConsumer, err := consumer.NewBaseKKConsumer("it-consumer", consumer.KafkaGroupConsumerConfig{
		BootstrapServers: strings.Split(kafkaURL, ","),
		Topics:           []string{topic},
		GroupID:          randomGroup("base-group"),
		AutoOffsetReset:  "earliest",
	}, consumer.KafkaConsumerRetryPolicy{
		MaxFailure:    2,
		BackoffBase:   5 * time.Millisecond,
		BackoffMax:    5 * time.Millisecond,
		BackoffFactor: 1,
	})
	assert.NoError(t, err)
	defer baseConsumer.Close()

	pre := int32(0)
	post := int32(0)
	handled := make(chan struct{}, 1)

	err = baseConsumer.StartConsume(
		func(ctx context.Context, message *kafka.Message) error {
			_ = ctx
			_ = message
			select {
			case handled <- struct{}{}:
			default:
			}
			return nil
		},
		func() { atomic.AddInt32(&pre, 1) },
		func() { atomic.AddInt32(&post, 1) },
	)
	assert.NoError(t, err)

	writer := producer.GetKafkaWriter()
	assert.NotNil(t, writer)
	msg := producer.Message{Topic: topic, Key: []byte("key"), Value: []byte("value")}
	err = writer.WriteMessages(context.Background(), msg)
	assert.Nil(t, err)

	select {
	case <-handled:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for message handling")
	}

	err = baseConsumer.Close()
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int32(1), pre)
	assert.Equal(t, int32(1), post)
}
