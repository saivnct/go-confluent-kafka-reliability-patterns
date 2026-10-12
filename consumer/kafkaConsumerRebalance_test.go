package consumer

import (
	"errors"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
)

type mockRebalanceAssigner struct {
	protocol  string
	lost      bool
	assignErr error
	calls     []string
	got       []kafka.TopicPartition
}

func (m *mockRebalanceAssigner) Assign(partitions []kafka.TopicPartition) error {
	m.calls = append(m.calls, "Assign")
	m.got = partitions
	return m.assignErr
}

func (m *mockRebalanceAssigner) Unassign() error {
	m.calls = append(m.calls, "Unassign")
	return nil
}

func (m *mockRebalanceAssigner) IncrementalAssign(partitions []kafka.TopicPartition) error {
	m.calls = append(m.calls, "IncrementalAssign")
	m.got = partitions
	return m.assignErr
}

func (m *mockRebalanceAssigner) IncrementalUnassign(partitions []kafka.TopicPartition) error {
	m.calls = append(m.calls, "IncrementalUnassign")
	m.got = partitions
	return nil
}

func (m *mockRebalanceAssigner) GetRebalanceProtocol() string { return m.protocol }

func (m *mockRebalanceAssigner) AssignmentLost() bool { return m.lost }

func testPartitions(ids ...int32) []kafka.TopicPartition {
	topic := "t"
	out := make([]kafka.TopicPartition, 0, len(ids))
	for _, id := range ids {
		out = append(out, kafka.TopicPartition{Topic: &topic, Partition: id})
	}
	return out
}

// hooksRecorder records hook calls into the same call list as the assigner, to check the order.
func hooksRecorder(m *mockRebalanceAssigner, lostSeen *bool) KafkaRebalanceHooks {
	return KafkaRebalanceHooks{
		OnAssigned: func(partitions []kafka.TopicPartition) {
			m.calls = append(m.calls, "OnAssigned")
		},
		OnRevoked: func(partitions []kafka.TopicPartition, lost bool) {
			m.calls = append(m.calls, "OnRevoked")
			*lostSeen = lost
		},
	}
}

func TestApplyRebalance_Eager_AssignThenHook(t *testing.T) {
	m := &mockRebalanceAssigner{protocol: "EAGER"}
	lost := false
	partitions := testPartitions(0, 1)

	err := applyRebalance(m, kafka.AssignedPartitions{Partitions: partitions}, hooksRecorder(m, &lost))
	assert.NoError(t, err)
	assert.Equal(t, []string{"Assign", "OnAssigned"}, m.calls)
	assert.Equal(t, partitions, m.got)
}

func TestApplyRebalance_Eager_HookThenUnassign(t *testing.T) {
	m := &mockRebalanceAssigner{protocol: "EAGER"}
	lost := true

	err := applyRebalance(m, kafka.RevokedPartitions{Partitions: testPartitions(0, 1)}, hooksRecorder(m, &lost))
	assert.NoError(t, err)
	assert.Equal(t, []string{"OnRevoked", "Unassign"}, m.calls)
	assert.False(t, lost)
}

func TestApplyRebalance_Cooperative_UsesIncremental(t *testing.T) {
	m := &mockRebalanceAssigner{protocol: rebalanceProtocolCooperative}
	lost := false
	added := testPartitions(2)

	err := applyRebalance(m, kafka.AssignedPartitions{Partitions: added}, hooksRecorder(m, &lost))
	assert.NoError(t, err)
	assert.Equal(t, added, m.got)

	moved := testPartitions(1)
	err = applyRebalance(m, kafka.RevokedPartitions{Partitions: moved}, hooksRecorder(m, &lost))
	assert.NoError(t, err)
	assert.Equal(t, moved, m.got)
	assert.Equal(t, []string{"IncrementalAssign", "OnAssigned", "OnRevoked", "IncrementalUnassign"}, m.calls)
}

func TestApplyRebalance_LostAssignment_PassedToHook(t *testing.T) {
	m := &mockRebalanceAssigner{protocol: "EAGER", lost: true}
	lost := false

	err := applyRebalance(m, kafka.RevokedPartitions{Partitions: testPartitions(0)}, hooksRecorder(m, &lost))
	assert.NoError(t, err)
	assert.True(t, lost)
}

func TestApplyRebalance_AssignError_NoHook(t *testing.T) {
	m := &mockRebalanceAssigner{protocol: "EAGER", assignErr: errors.New("boom")}
	lost := false

	err := applyRebalance(m, kafka.AssignedPartitions{Partitions: testPartitions(0)}, hooksRecorder(m, &lost))
	assert.Error(t, err)
	assert.Equal(t, []string{"Assign"}, m.calls)
}

func TestApplyRebalance_NoHooks(t *testing.T) {
	m := &mockRebalanceAssigner{protocol: "EAGER"}

	assert.NoError(t, applyRebalance(m, kafka.AssignedPartitions{Partitions: testPartitions(0)}, KafkaRebalanceHooks{}))
	assert.NoError(t, applyRebalance(m, kafka.RevokedPartitions{Partitions: testPartitions(0)}, KafkaRebalanceHooks{}))
	assert.Equal(t, []string{"Assign", "Unassign"}, m.calls)
}
