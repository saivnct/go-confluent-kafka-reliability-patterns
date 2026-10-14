package consumer

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	kkerror "github.com/saivnct/go-confluent-kafka-reliability-patterns/kkErrors"
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

// DefaultDLTMaxWriteAttempts is used when KafkaConsumerDLTPolicy.MaxWriteAttempts is not set (<= 0).
const DefaultDLTMaxWriteAttempts = 10

type KafkaConsumerDLTPolicy struct {
	Enabled bool
	Topic   string
	Writer  DLTWriter
	// MaxWriteAttempts is how many times the dead-letter write is tried for one message, including the first
	// try, with the retry policy backoff between tries. When every try fails, the message is not committed and
	// the consume loop stops with kkErrors.KKDLTWriteFailedErr. <= 0 means DefaultDLTMaxWriteAttempts.
	MaxWriteAttempts int
}

func (p KafkaConsumerDLTPolicy) normalize(sourceTopic string) KafkaConsumerDLTPolicy {
	if !p.Enabled {
		return p
	}
	if p.Topic == "" {
		p.Topic = sourceTopic + ".DLT"
	}
	if p.MaxWriteAttempts <= 0 {
		p.MaxWriteAttempts = DefaultDLTMaxWriteAttempts
	}
	return p
}

// publishToDLTIfEnabled writes the message to the dead-letter topic, trying up to policy.MaxWriteAttempts times
// with the retry policy backoff between tries. Only the write is retried, the handler is not run again.
// It returns an error wrapping kkErrors.KKDLTWriteFailedErr when every try fails, or the context error when ctx
// ends during a backoff.
func publishToDLTIfEnabled(
	ctx context.Context,
	dltPolicy KafkaConsumerDLTPolicy,
	retryPolicy KafkaConsumerRetryPolicy,
	message *kafka.Message,
	attempts int,
	handleErr error,
) error {
	if !dltPolicy.Enabled {
		return nil
	}
	if dltPolicy.Writer == nil {
		return fmt.Errorf("%w: dlt writer is required when dlt is enabled", kkerror.KKDLTWriteFailedErr)
	}
	if dltPolicy.Topic == "" {
		return fmt.Errorf("%w: dlt topic is required when dlt is enabled", kkerror.KKDLTWriteFailedErr)
	}

	dltHeaders := append(connMessageHeadersFromKafka(message.Headers),
		kafka.Header{Key: dltHeaderOriginalTopic, Value: []byte(topicFromKafkaMessage(message))},
		kafka.Header{Key: dltHeaderOriginalPartition, Value: []byte(strconv.FormatInt(int64(message.TopicPartition.Partition), 10))},
		kafka.Header{Key: dltHeaderOriginalOffset, Value: []byte(strconv.FormatInt(int64(message.TopicPartition.Offset), 10))},
		kafka.Header{Key: dltHeaderFailureReason, Value: []byte(handleErr.Error())},
		kafka.Header{Key: dltHeaderFailureAttempts, Value: []byte(strconv.Itoa(attempts))},
		kafka.Header{Key: dltHeaderFailedAt, Value: []byte(time.Now().UTC().Format(time.RFC3339Nano))},
	)

	dltMessage := producer.Message{
		Topic:   dltPolicy.Topic,
		Key:     message.Key,
		Value:   message.Value,
		Headers: dltHeaders,
		Time:    time.Now().UTC(),
	}

	for writeAttempt := 1; ; writeAttempt++ {
		err := dltPolicy.Writer.WriteMessages(ctx, dltMessage)
		if err == nil {
			return nil
		}
		if writeAttempt >= dltPolicy.MaxWriteAttempts {
			return fmt.Errorf("%w after %d attempts (topic %s, partition %d, offset %d): %w",
				kkerror.KKDLTWriteFailedErr, writeAttempt, topicFromKafkaMessage(message),
				message.TopicPartition.Partition, message.TopicPartition.Offset, err)
		}
		if err := waitBackoffOrCancel(ctx, retryPolicy.backoffForAttempt(writeAttempt)); err != nil {
			return err
		}
	}
}
