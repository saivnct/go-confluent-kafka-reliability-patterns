package consumer

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/saivnct/go-confluent-kafka-reliability-patterns/producer"
)

const (
	dltHeaderOriginalTopic     = "x-dlt-original-topic"
	dltHeaderOriginalPartition = "x-dlt-original-partition"
	dltHeaderOriginalOffset    = "x-dlt-original-offset"
	dltHeaderFailureReason     = "x-dlt-failure-reason"
	dltHeaderFailureAttempts   = "x-dlt-failure-attempts"
	dltHeaderFailedAt          = "x-dlt-failed-at"
)

type DLTWriter interface {
	WriteMessages(ctx context.Context, msgs ...producer.Message) error
}

type KafkaConsumerDLTPolicy struct {
	Enabled bool
	Topic   string
	Writer  DLTWriter
}

func (p KafkaConsumerDLTPolicy) normalize(sourceTopic string) KafkaConsumerDLTPolicy {
	if !p.Enabled {
		return p
	}
	if p.Topic == "" {
		p.Topic = sourceTopic + ".DLT"
	}
	return p
}

func publishToDLTIfEnabled(
	ctx context.Context,
	policy KafkaConsumerDLTPolicy,
	message *kafka.Message,
	attempts int,
	handleErr error,
) error {
	if !policy.Enabled {
		return nil
	}
	if policy.Writer == nil {
		return fmt.Errorf("dlt writer is required when dlt is enabled")
	}
	if policy.Topic == "" {
		return fmt.Errorf("dlt topic is required when dlt is enabled")
	}

	dltHeaders := append(connMessageHeadersFromKafka(message.Headers),
		kafka.Header{Key: dltHeaderOriginalTopic, Value: []byte(topicFromKafkaMessage(message))},
		kafka.Header{Key: dltHeaderOriginalPartition, Value: []byte(strconv.FormatInt(int64(message.TopicPartition.Partition), 10))},
		kafka.Header{Key: dltHeaderOriginalOffset, Value: []byte(strconv.FormatInt(int64(message.TopicPartition.Offset), 10))},
		kafka.Header{Key: dltHeaderFailureReason, Value: []byte(handleErr.Error())},
		kafka.Header{Key: dltHeaderFailureAttempts, Value: []byte(strconv.Itoa(attempts))},
		kafka.Header{Key: dltHeaderFailedAt, Value: []byte(time.Now().UTC().Format(time.RFC3339Nano))},
	)

	return policy.Writer.WriteMessages(ctx, producer.Message{
		Topic:   policy.Topic,
		Key:     message.Key,
		Value:   message.Value,
		Headers: dltHeaders,
		Time:    time.Now().UTC(),
	})
}
