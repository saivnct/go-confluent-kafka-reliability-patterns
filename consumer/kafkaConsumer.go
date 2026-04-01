package consumer

import (
	"context"
	"errors"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/saivnct/go-confluent-kafka-reliability-patterns/kkErrors"
)

type KafkaPreConsumeHook func()
type KafkaPostConsumeHook func()

type BaseKKConsumer struct {
	Name        string
	Reader      *kafka.Consumer
	Topics      []string
	GroupID     string
	RetryPolicy KafkaConsumerRetryPolicy
	DLTPolicy   KafkaConsumerDLTPolicy

	mu        sync.Mutex
	runCancel context.CancelFunc
	IsRunning bool
}

func (b *BaseKKConsumer) StartConsume(
	handler KafkaMessageHandler,
	preConsumeHook KafkaPreConsumeHook,
	postConsumeHook KafkaPostConsumeHook,
) error {
	b.mu.Lock()
	if b.IsRunning {
		b.mu.Unlock()
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	b.runCancel = cancel
	b.IsRunning = true
	b.mu.Unlock()

	err := b.consumeMsg(ctx, handler, preConsumeHook, postConsumeHook)
	if err != nil {
		cancel()

		b.mu.Lock()
		b.runCancel = nil
		b.IsRunning = false
		b.mu.Unlock()

		return err
	}

	return nil
}

func (b *BaseKKConsumer) Close() error {
	b.mu.Lock()
	runCancel := b.runCancel
	b.runCancel = nil
	b.IsRunning = false
	b.mu.Unlock()

	if runCancel != nil {
		runCancel()
	}

	if b.Reader != nil {
		err := b.Reader.Close()
		return err
	}

	return nil
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
		if postConsumeHook != nil {
			defer func() {
				postConsumeHook()
			}()
		}

		retryPolicy := kkConsumer.RetryPolicy.normalize()
		dltPolicy := kkConsumer.DLTPolicy

		log.Println("Start Kafka Consuming", "BaseKKConsumer:", b.Name, "topics:", kkConsumer.Topics, "groupID:", kkConsumer.GroupID)

		pollTimeoutMs := int(retryPolicy.PollTimeout.Milliseconds())
		if pollTimeoutMs <= 0 {
			pollTimeoutMs = 200
		}

		for {
			if err := ctx.Err(); err != nil {
				log.Println("Stop KK Consumer loop", "BaseKKConsumer:", b.Name, "error:", err)
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
						log.Println("Stop KK Consumer loop on terminal process error", "BaseKKConsumer:", b.Name, "error:", err)
						return
					}

					log.Println("Failed to process Kafka message", "BaseKKConsumer:", b.Name, "topic:", topicFromKafkaMessage(e), "partition:", e.TopicPartition.Partition, "offset:", e.TopicPartition.Offset, "error:", err)

					if err := sleepWithContext(ctx, retryPolicy.SleepOnFetchErr); err != nil {
						log.Println("Stop KK Consumer while waiting after process error", "BaseKKConsumer:", b.Name, "error:", err)
						return
					}
				}
			case kafka.Error:
				if isTerminalConsumerErr(ctx, e) {
					log.Println("Stop KK Consumer loop on terminal process error", "BaseKKConsumer:", b.Name, "error:", e)
					return
				}

				log.Println("Kafka consumer error", "BaseKKConsumer:", b.Name, "error:", e)

				if err := sleepWithContext(ctx, retryPolicy.SleepOnFetchErr); err != nil {
					log.Println("Stop KK Consumer while waiting after consumer error", "BaseKKConsumer:", b.Name, "error:", err)
					return
				}
			case kafka.AssignedPartitions:
				log.Println("Kafka partitions assigned", "BaseKKConsumer:", b.Name, "partitions:", e.Partitions)

				if err := kkConsumer.Reader.Assign(e.Partitions); err != nil {
					log.Println("Failed to assign partitions", "BaseKKConsumer:", b.Name, "error:", err)
				}
			case kafka.RevokedPartitions:
				log.Println("Kafka partitions revoked", "BaseKKConsumer:", b.Name, "partitions:", e.Partitions)
				if err := kkConsumer.Reader.Unassign(); err != nil {
					log.Println("Failed to unassign partitions", "BaseKKConsumer:", b.Name, "error:", err)
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
