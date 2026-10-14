package consumer

import (
	"context"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	kkerror "github.com/saivnct/go-confluent-kafka-reliability-patterns/kkErrors"
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

// ProcessKafkaMessageWithRetryAndOptions runs handler against a message with retries, writes it to the
// dead-letter topic when the handler gives up (if enabled), and commits the offset after it.
// When the dead-letter write fails on every attempt, the message is not committed and the error wraps
// kkErrors.KKDLTWriteFailedErr. When only the commit fails, the error wraps kkErrors.KKCommitFailedErr.
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
			return commitMessage(committer, message)
		}

		terminal := kkerror.IsNotRetryable(err) || !kkerror.IsRetryable(err)
		if terminal {
			if err := publishToDLTIfEnabled(ctx, dltPolicy, retryPolicy, message, failures+1, err); err != nil {
				return err
			}
			return commitMessage(committer, message)
		}

		if retryPolicy.MaxFailure >= 0 && failures+1 >= retryPolicy.MaxFailure {
			if err := publishToDLTIfEnabled(ctx, dltPolicy, retryPolicy, message, failures+1, err); err != nil {
				return err
			}
			return commitMessage(committer, message)
		}

		backoff := retryPolicy.backoffForAttempt(failures + 1)

		if err := waitBackoffOrCancel(ctx, backoff); err != nil {
			return err
		}
	}
}

// commitMessage commits the offset after the message and wraps a failure with kkErrors.KKCommitFailedErr.
func commitMessage(committer KafkaMessageCommitter, message *kafka.Message) error {
	if _, err := committer.CommitMessage(message); err != nil {
		return fmt.Errorf("%w (partition %d, offset %d): %w", kkerror.KKCommitFailedErr,
			message.TopicPartition.Partition, message.TopicPartition.Offset, err)
	}
	return nil
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
