package consumer

import (
	"context"
	"errors"
	"log"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/saivnct/go-confluent-kafka-reliability-patterns/kkErrors"
)

func (b *BaseKKConsumer) completeRun(runErr error) {
	b.mu.Lock()
	b.runCancel = nil
	b.IsRunning = false
	b.runErr = runErr
	doneCh := b.doneCh
	b.mu.Unlock()

	if doneCh != nil {
		select {
		case <-doneCh:
			// Channel already closed by another termination path.
		default:
			close(doneCh)
		}
	}
}

func (b *BaseKKConsumer) consumeMsg(
	ctx context.Context,
	consumerHandler KafkaMessageHandler,
	preConsumeHook KafkaPreConsumeHook,
	postConsumeHook KafkaPostConsumeHook,
) error {
	if b.Reader == nil {
		return kkErrors.KKConsumerNotInitErr
	}
	if consumerHandler == nil {
		return errors.New("consumer handler is required")
	}

	if preConsumeHook != nil {
		preConsumeHook()
	}

	go func(kkConsumer *BaseKKConsumer) {
		runErr := error(nil)
		defer func() {
			if runErr == nil {
				runErr = ctx.Err()
			}
			kkConsumer.completeRun(runErr)
		}()

		if postConsumeHook != nil {
			defer func() {
				postConsumeHook()
			}()
		}

		retryPolicy := kkConsumer.RetryPolicy.normalize()
		dltPolicy := kkConsumer.DLTPolicy

		pollTimeoutMs := int(retryPolicy.PollTimeout.Milliseconds())
		if pollTimeoutMs <= 0 {
			pollTimeoutMs = 200
		}

		isLogEnable := b.IsLogEnable

		for {
			if err := ctx.Err(); err != nil {
				runErr = err
				return
			}

			event := kkConsumer.Reader.Poll(pollTimeoutMs)
			if event == nil {
				continue
			}

			switch e := event.(type) {
			case *kafka.Message:
				// retries of this message keep the same poll time, see PolledAt
				msgCtx := WithPolledAt(ctx, time.Now())
				if err := ProcessKafkaMessageWithRetryAndOptions(msgCtx, kkConsumer.Reader, e, consumerHandler, retryPolicy, dltPolicy); err != nil {
					if isTerminalConsumerErr(ctx, err) {
						runErr = err
						return
					}

					// the message could not be handled nor written to the dead-letter topic: stop without
					// committing, so a restart reads it again instead of skipping it
					if errors.Is(err, kkErrors.KKDLTWriteFailedErr) {
						if isLogEnable {
							log.Println("Stop consuming: dead-letter write failed", "BaseKKConsumer:", b.Name, "topic:", topicFromKafkaMessage(e), "partition:", e.TopicPartition.Partition, "offset:", e.TopicPartition.Offset, "error:", err)
						}
						runErr = err
						return
					}

					// the message was handled; a later commit covers its offset
					if errors.Is(err, kkErrors.KKCommitFailedErr) {
						if isLogEnable {
							log.Println("Kafka offset commit failed, continue", "BaseKKConsumer:", b.Name, "topic:", topicFromKafkaMessage(e), "partition:", e.TopicPartition.Partition, "offset:", e.TopicPartition.Offset, "error:", err)
						}
						continue
					}

					if isLogEnable {
						log.Println("Failed to process Kafka message", "BaseKKConsumer:", b.Name, "topic:", topicFromKafkaMessage(e), "partition:", e.TopicPartition.Partition, "offset:", e.TopicPartition.Offset, "error:", err)
					}

					if err := sleepWithContext(ctx, retryPolicy.SleepOnFetchErr); err != nil {
						runErr = err
						return
					}
				}
			case kafka.Error:
				if isTerminalConsumerErr(ctx, e) {
					runErr = e
					return
				}

				if isLogEnable {
					log.Println("Kafka consumer error", "BaseKKConsumer:", b.Name, "error:", e)
				}

				if err := sleepWithContext(ctx, retryPolicy.SleepOnFetchErr); err != nil {
					runErr = err
					return
				}
			}
		}
	}(b)

	return nil
}

func isTerminalConsumerErr(ctx context.Context, err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if ctx.Err() != nil {
		return true
	}

	var kafkaErr kafka.Error
	if errors.As(err, &kafkaErr) {
		if kafkaErr.IsFatal() {
			return true
		}
	}

	errText := strings.ToLower(err.Error())
	if strings.Contains(errText, "closed") || strings.Contains(errText, "terminated") {
		return true
	}

	return false
}

func sleepWithContext(ctx context.Context, wait time.Duration) error {
	if wait <= 0 {
		return ctx.Err()
	}

	timer := time.NewTimer(wait)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
