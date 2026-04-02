package consumer

import (
	"context"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/saivnct/go-confluent-kafka-reliability-patterns/util"
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
	doneCh    chan struct{}
	runErr    error
	IsRunning bool
}

func NewBaseKKConsumer(name string, cfg KafkaGroupConsumerConfig, retryPolicy KafkaConsumerRetryPolicy, dltPolicy KafkaConsumerDLTPolicy) (*BaseKKConsumer, error) {
	c, err := NewKafkaGroupConsumer(cfg)
	if err != nil {
		return nil, err
	}

	return &BaseKKConsumer{
		Name:        name,
		Reader:      c,
		Topics:      append([]string(nil), cfg.Topics...),
		GroupID:     cfg.GroupID,
		RetryPolicy: retryPolicy,
		DLTPolicy:   dltPolicy,
		doneCh:      util.GetClosedSignalChannel(),
	}, nil
}

// GetConsumeDoneChan returns a run-scoped channel that closes when the current consume loop stops.
//
// StartConsume creates a new channel for each successful run, so callers should re-read
// GetConsumeDoneChan after each StartConsume call.
func (b *BaseKKConsumer) GetConsumeDoneChan() <-chan struct{} {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.doneCh
}

// ConsumeErr returns the terminal error from the latest consume run.
//
// It is reset to nil on StartConsume, then populated when that run terminates.
func (b *BaseKKConsumer) ConsumeErr() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.runErr
}

// StartConsume starts the consume loop if it is not already running.
//
// Each new run resets ConsumeErr and creates a new GetConsumeDoneChan channel.
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
	b.doneCh = make(chan struct{})
	b.runErr = nil
	b.IsRunning = true
	b.mu.Unlock()

	err := b.consumeMsg(ctx, handler, preConsumeHook, postConsumeHook)
	if err != nil {
		cancel()
		b.completeRun(err)

		return err
	}

	return nil
}

func (b *BaseKKConsumer) Close() error {
	b.mu.Lock()
	runCancel := b.runCancel
	b.runCancel = nil
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
