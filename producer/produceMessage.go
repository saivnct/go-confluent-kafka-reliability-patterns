package producer

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// WriteMessages produces a batch and waits for delivery reports for all messages
// that were accepted by Produce.
//
// It returns enqueue-time errors (for example invalid topic, queue/full produce
// failures) and delivery report errors. If both produce and delivery paths fail
// in the same call, it returns a joined error.
func (w *KafkaWriter) WriteMessages(ctx context.Context, msgs ...Message) error {
	if w == nil || w.producer == nil {
		return fmt.Errorf("kafka writer is not initialized")
	}
	if len(msgs) == 0 {
		return nil
	}

	deliveryChan := make(chan kafka.Event, len(msgs))

	produced := 0
	var produceErr error
	for i, msg := range msgs {
		if msg.Topic == "" {
			produceErr = fmt.Errorf("kafka topic is required at index %d", i)
			break
		}

		if err := w.produceWithRetryOnLocalQueueFull(ctx, msg.toKafkaMessage(w.idempotencyHeaderKey), deliveryChan); err != nil {
			produceErr = fmt.Errorf("produce message at index %d failed: %w", i, err)
			break
		}
		produced++
	}

	if produced == 0 {
		return produceErr
	}

	deliveryErr := w.waitForDeliveries(ctx, deliveryChan, produced)
	if produceErr != nil && deliveryErr != nil {
		return errors.Join(produceErr, deliveryErr)
	}
	if produceErr != nil {
		return produceErr
	}
	return deliveryErr
}

// WriteMessagesAsync runs WriteMessages in a goroutine and returns a result channel.
//
// The returned channel emits exactly one error result (nil on success), then is
// closed. It preserves WriteMessages semantics for delivery waiting and errors.
func (w *KafkaWriter) WriteMessagesAsync(ctx context.Context, msgs ...Message) <-chan error {
	result := make(chan error, 1)
	batch := cloneMessageBatch(msgs)

	go func() {
		defer close(result)
		result <- w.WriteMessages(ctx, batch...)
	}()

	return result
}

// WriteMessagesFireAndForget enqueues messages without waiting for delivery reports.
//
// It returns only enqueue-time errors from Produce and does not guarantee broker
// delivery. Since this writer enables "go.delivery.reports", delivery events for
// these messages are emitted on producer.Events(). Callers should run an events
// drain loop (or disable delivery reports) when using this method heavily.
func (w *KafkaWriter) WriteMessagesFireAndForget(ctx context.Context, msgs ...Message) error {
	if w == nil || w.producer == nil {
		return fmt.Errorf("kafka writer is not initialized")
	}
	if len(msgs) == 0 {
		return nil
	}

	for i, msg := range msgs {
		if err := ctx.Err(); err != nil {
			return err
		}
		if msg.Topic == "" {
			return fmt.Errorf("kafka topic is required at index %d", i)
		}

		if err := w.producer.Produce(msg.toKafkaMessage(w.idempotencyHeaderKey), nil); err != nil {
			return fmt.Errorf("produce message at index %d failed: %w", i, err)
		}
	}

	return nil
}

func (w *KafkaWriter) produceWithRetryOnLocalQueueFull(ctx context.Context, message *kafka.Message, deliveryChan chan kafka.Event) error {
	for {
		err := w.producer.Produce(message, deliveryChan)
		if err == nil {
			return nil
		}

		kafkaErr, ok := err.(kafka.Error)
		if !ok || kafkaErr.Code() != kafka.ErrQueueFull {
			return err
		}

		log.Println("local producer queue is full, retrying produce after a short wait...")

		// Backpressure when local producer queue is full, wait some time for messages
		// to be delivered then try again. This module version does not expose Poll,
		// so use a short Flush to serve delivery events and free queue slots.
		_ = w.producer.Flush(10)
		if err := waitOrCancel(ctx, 5*time.Millisecond); err != nil {
			return err
		}
	}
}

func (w *KafkaWriter) waitForDeliveries(ctx context.Context, deliveryChan chan kafka.Event, expected int) error {
	timeout := w.deliveryTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	deliveryCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var firstErr error
	for received := 0; received < expected; {
		select {
		case <-deliveryCtx.Done():
			if firstErr != nil {
				return errors.Join(deliveryCtx.Err(), firstErr)
			}
			return deliveryCtx.Err()
		case event := <-deliveryChan:
			deliveryMsg, ok := event.(*kafka.Message)
			if !ok {
				if firstErr == nil {
					firstErr = fmt.Errorf("unexpected kafka delivery event %T", event)
				}
				continue
			}
			received++
			if deliveryMsg.TopicPartition.Error != nil && firstErr == nil {
				firstErr = deliveryMsg.TopicPartition.Error
			}
		}
	}

	return firstErr
}

func cloneMessageBatch(msgs []Message) []Message {
	if len(msgs) == 0 {
		return nil
	}

	cloned := make([]Message, len(msgs))
	for i, msg := range msgs {
		cloned[i] = Message{
			Topic:                 msg.Topic,
			Time:                  msg.Time,
			UseDedicatedPartition: msg.UseDedicatedPartition,
			Partition:             msg.Partition,
			Headers:               cloneHeaders(msg.Headers),
		}
		if len(msg.Key) > 0 {
			cloned[i].Key = append([]byte(nil), msg.Key...)
		}
		if len(msg.Value) > 0 {
			cloned[i].Value = append([]byte(nil), msg.Value...)
		}
	}

	return cloned
}

func waitOrCancel(ctx context.Context, wait time.Duration) error {
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
