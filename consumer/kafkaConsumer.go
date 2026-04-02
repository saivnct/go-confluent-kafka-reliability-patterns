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
				if err := ProcessKafkaMessageWithRetryAndOptions(ctx, kkConsumer.Reader, e, consumerHandler, retryPolicy, dltPolicy); err != nil {
					if isTerminalConsumerErr(ctx, err) {
						runErr = err
						return
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
			case kafka.AssignedPartitions:
				if isLogEnable {
					log.Println("Kafka partitions assigned", "BaseKKConsumer:", b.Name, "partitions:", e.Partitions)
				}

				if err := kkConsumer.Reader.Assign(e.Partitions); err != nil {
					if isLogEnable {
						log.Println("Failed to assign partitions", "BaseKKConsumer:", b.Name, "error:", err)
					}
				}
			case kafka.RevokedPartitions:
				if isLogEnable {
					log.Println("Kafka partitions revoked", "BaseKKConsumer:", b.Name, "partitions:", e.Partitions)
				}

				if err := kkConsumer.Reader.Unassign(); err != nil {
					if isLogEnable {
						log.Println("Failed to unassign partitions", "BaseKKConsumer:", b.Name, "error:", err)
					}
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
