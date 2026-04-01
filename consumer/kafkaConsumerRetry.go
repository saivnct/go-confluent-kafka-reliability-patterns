package consumer

import (
	"context"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	kkerror "github.com/saivnct/kafka-cf-reliablity/kkErrors"
)

type KafkaMessageCommitter interface {
	CommitMessage(msg *kafka.Message) ([]kafka.TopicPartition, error)
}

type KafkaMessageHandler func(ctx context.Context, message *kafka.Message) error

// ProcessKafkaMessageWithRetry runs handler against a message and commits offsets based on retry policy.
func ProcessKafkaMessageWithRetry(
	ctx context.Context,
	committer KafkaMessageCommitter,
	message *kafka.Message,
	handler KafkaMessageHandler,
	policy KafkaConsumerRetryPolicy,
) error {
	return ProcessKafkaMessageWithRetryAndOptions(ctx, committer, message, handler, policy, KafkaConsumerDLTPolicy{})
}

func ProcessKafkaMessageWithRetryAndOptions(
	ctx context.Context,
	committer KafkaMessageCommitter,
	message *kafka.Message,
	handler KafkaMessageHandler,
	retryPolicy KafkaConsumerRetryPolicy,
	dltPolicy KafkaConsumerDLTPolicy,
) error {
	if message == nil {
		return fmt.Errorf("kafka message is required")
	}

	topic := topicFromKafkaMessage(message)

	retryPolicy = retryPolicy.normalize()
	dltPolicy = dltPolicy.normalize(topic)

	for failures := 0; ; failures++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		err := handler(ctx, message)
		if err == nil {
			_, commitErr := committer.CommitMessage(message)
			return commitErr
		}

		terminal := kkerror.IsNotRetryable(err) || !kkerror.IsRetryable(err)
		if terminal {
			if err := publishToDLTIfEnabled(ctx, dltPolicy, message, failures+1, err); err != nil {
				return err
			}
			_, commitErr := committer.CommitMessage(message)
			return commitErr
		}

		if retryPolicy.MaxFailure >= 0 && failures+1 >= retryPolicy.MaxFailure {
			if err := publishToDLTIfEnabled(ctx, dltPolicy, message, failures+1, err); err != nil {
				return err
			}
			_, commitErr := committer.CommitMessage(message)
			return commitErr
		}

		backoff := retryPolicy.backoffForAttempt(failures + 1)

		if err := waitBackoffOrCancel(ctx, backoff); err != nil {
			return err
		}
	}
}

func connMessageHeadersFromKafka(headers []kafka.Header) []kafka.Header {
	if len(headers) == 0 {
		return nil
	}
	cloned := make([]kafka.Header, len(headers))
	copy(cloned, headers)
	return cloned
}

func topicFromKafkaMessage(message *kafka.Message) string {
	if message == nil || message.TopicPartition.Topic == nil {
		return ""
	}
	return *message.TopicPartition.Topic
}

func waitBackoffOrCancel(ctx context.Context, backoff time.Duration) error {
	if backoff <= 0 {
		return ctx.Err()
	}

	timer := time.NewTimer(backoff)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
