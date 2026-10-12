package consumer

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// KafkaPartitionsAssignedHook is called after partitions are assigned to the consumer.
// With the cooperative protocol, partitions holds only the newly added partitions (it can be empty).
type KafkaPartitionsAssignedHook func(partitions []kafka.TopicPartition)

// KafkaPartitionsRevokedHook is called before partitions are taken away from the consumer.
// With the cooperative protocol, partitions holds only the partitions that move.
// lost is true when the assignment was lost instead of revoked cleanly (for example the consumer
// missed the session timeout): the group may already have given the partitions to another consumer.
type KafkaPartitionsRevokedHook func(partitions []kafka.TopicPartition, lost bool)

// KafkaRebalanceHooks are optional callbacks for partition changes of a BaseKKConsumer.
type KafkaRebalanceHooks struct {
	OnAssigned KafkaPartitionsAssignedHook
	OnRevoked  KafkaPartitionsRevokedHook
}

// rebalanceProtocolCooperative is the value of kafka.Consumer.GetRebalanceProtocol() with an
// incremental strategy such as cooperative-sticky.
const rebalanceProtocolCooperative = "COOPERATIVE"

// rebalanceAssigner is the part of *kafka.Consumer that applies a rebalance.
type rebalanceAssigner interface {
	Assign(partitions []kafka.TopicPartition) error
	Unassign() error
	IncrementalAssign(partitions []kafka.TopicPartition) error
	IncrementalUnassign(partitions []kafka.TopicPartition) error
	GetRebalanceProtocol() string
	AssignmentLost() bool
}

// onRebalance is the kafka.RebalanceCb of the consumer. The Kafka client calls it from Poll.
func (b *BaseKKConsumer) onRebalance(c *kafka.Consumer, event kafka.Event) error {
	if b.IsLogEnable {
		log.Println("Kafka rebalance", "BaseKKConsumer:", b.Name, "event:", event)
	}

	err := applyRebalance(c, event, b.RebalanceHooks)
	if err != nil && b.IsLogEnable {
		log.Println("Failed to apply Kafka rebalance", "BaseKKConsumer:", b.Name, "error:", err)
	}
	return err
}

// applyRebalance assigns or revokes the partitions of event, with the eager or the cooperative
// protocol, and calls the hooks: OnAssigned after the assign, OnRevoked before the revoke.
func applyRebalance(assigner rebalanceAssigner, event kafka.Event, hooks KafkaRebalanceHooks) error {
	cooperative := assigner.GetRebalanceProtocol() == rebalanceProtocolCooperative

	switch e := event.(type) {
	case kafka.AssignedPartitions:
		var err error
		if cooperative {
			err = assigner.IncrementalAssign(e.Partitions)
		} else {
			err = assigner.Assign(e.Partitions)
		}
		if err != nil {
			return fmt.Errorf("assign partitions: %w", err)
		}
		if hooks.OnAssigned != nil {
			hooks.OnAssigned(e.Partitions)
		}
	case kafka.RevokedPartitions:
		if hooks.OnRevoked != nil {
			hooks.OnRevoked(e.Partitions, assigner.AssignmentLost())
		}
		var err error
		if cooperative {
			err = assigner.IncrementalUnassign(e.Partitions)
		} else {
			err = assigner.Unassign()
		}
		if err != nil {
			return fmt.Errorf("revoke partitions: %w", err)
		}
	}
	return nil
}
