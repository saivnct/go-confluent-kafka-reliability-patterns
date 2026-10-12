package test

import (
	"context"
	"fmt"
	"log"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/saivnct/go-confluent-kafka-reliability-patterns/admin"
	"github.com/saivnct/go-confluent-kafka-reliability-patterns/consumer"
	kkerror "github.com/saivnct/go-confluent-kafka-reliability-patterns/kkErrors"
	"github.com/saivnct/go-confluent-kafka-reliability-patterns/producer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const rebalanceWaitTimeout = 60 * time.Second

// partitionTracker follows the partitions a consumer owns, using only the rebalance hooks.
// It works for both protocols: eager revokes and re-assigns everything, cooperative only the moved partitions.
type partitionTracker struct {
	mu      sync.Mutex
	owned   map[int32]bool
	revokes [][]int32
}

func newPartitionTracker() *partitionTracker {
	return &partitionTracker{owned: map[int32]bool{}}
}

func (p *partitionTracker) hooks() consumer.KafkaRebalanceHooks {
	return consumer.KafkaRebalanceHooks{
		OnAssigned: func(partitions []kafka.TopicPartition) {
			p.mu.Lock()
			defer p.mu.Unlock()
			for _, tp := range partitions {
				p.owned[tp.Partition] = true
			}
		},
		OnRevoked: func(partitions []kafka.TopicPartition, lost bool) {
			p.mu.Lock()
			defer p.mu.Unlock()
			revoked := []int32{}
			for _, tp := range partitions {
				delete(p.owned, tp.Partition)
				revoked = append(revoked, tp.Partition)
			}
			slices.Sort(revoked)
			p.revokes = append(p.revokes, revoked)
		},
	}
}

func (p *partitionTracker) ownedPartitions() []int32 {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := []int32{}
	for partition := range p.owned {
		out = append(out, partition)
	}
	slices.Sort(out)
	return out
}

func (p *partitionTracker) revokedSets() [][]int32 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.revokes)
}

type handledMessage struct {
	consumer  string
	partition int32
	value     string
	polledOK  bool
}

func newRebalanceConsumer(t *testing.T, name, topic, groupID, strategy string, tracker *partitionTracker) *consumer.BaseKKConsumer {
	t.Helper()
	additional := map[string]interface{}{}
	if strategy != "" {
		additional["partition.assignment.strategy"] = strategy
	}
	c, err := consumer.NewBaseKKConsumer(name, consumer.KafkaGroupConsumerConfig{
		BootstrapServers: strings.Split(mustKafkaURL(t), ","),
		Topics:           []string{topic},
		GroupID:          groupID,
		AutoOffsetReset:  "earliest",
		AdditionalConfig: additional,
	}, consumer.KafkaConsumerRetryPolicy{
		MaxFailure:    2,
		BackoffBase:   5 * time.Millisecond,
		BackoffMax:    5 * time.Millisecond,
		BackoffFactor: 1,
	}, consumer.KafkaConsumerDLTPolicy{})
	require.NoError(t, err)
	c.RebalanceHooks = tracker.hooks()
	return c
}

func startRebalanceConsumer(t *testing.T, c *consumer.BaseKKConsumer, results chan<- handledMessage) {
	t.Helper()
	err := c.StartConsume(func(ctx context.Context, message *kafka.Message) error {
		polledAt, ok := consumer.PolledAt(ctx)
		results <- handledMessage{
			consumer:  c.Name,
			partition: message.TopicPartition.Partition,
			value:     string(message.Value),
			polledOK:  ok && !polledAt.IsZero() && !polledAt.After(time.Now()),
		}
		return nil
	}, func() {}, func() {})
	require.NoError(t, err)
}

func waitOwned(t *testing.T, what string, check func() bool) {
	t.Helper()
	assert.Eventually(t, check, rebalanceWaitTimeout, 100*time.Millisecond, what)
}

// writeToEachPartition writes one message to each partition and returns the values by partition.
func writeToEachPartition(t *testing.T, topic string, partitions int32, round string) map[int32]string {
	t.Helper()
	values := map[int32]string{}
	for partition := int32(0); partition < partitions; partition++ {
		value := fmt.Sprintf("%s-p%d", round, partition)
		err := producer.GetKafkaWriter().WriteMessages(context.Background(), producer.Message{
			Topic:                 topic,
			Value:                 []byte(value),
			UseDedicatedPartition: true,
			Partition:             partition,
		})
		require.NoError(t, err)
		values[partition] = value
	}
	return values
}

// receiveValues waits for the given values and returns who handled each of them.
func receiveValues(t *testing.T, results <-chan handledMessage, values map[int32]string) map[string]handledMessage {
	t.Helper()
	want := map[string]bool{}
	for _, value := range values {
		want[value] = true
	}
	got := map[string]handledMessage{}
	deadline := time.After(rebalanceWaitTimeout)
	for len(got) < len(want) {
		select {
		case item := <-results:
			if want[item.value] {
				got[item.value] = item
			}
		case <-deadline:
			t.Fatalf("timed out waiting for messages, got %d of %d", len(got), len(want))
		}
	}
	return got
}

// runRebalanceScenario: consumer A owns both partitions, B joins and takes one, messages go to the owner,
// B leaves and A gets both back, A closes and its hooks see the last revoke.
// It returns the partition sets A saw revoked.
func runRebalanceScenario(t *testing.T, strategy string) [][]int32 {
	kafkaURL := mustKafkaURL(t)
	topic := randomTopic("rebalance")
	groupID := randomGroup("rebalance-group")
	const partitions = 2

	require.NoError(t, admin.CreateKafkaTopics(kafkaURL, admin.TopicConfig{Topic: topic, NumPartitions: partitions, ReplicationFactor: 1}))
	defer func() {
		_ = admin.DeleteKafkaTopics(kafkaURL, topic)
	}()

	results := make(chan handledMessage, 100)

	trackerA := newPartitionTracker()
	consumerA := newRebalanceConsumer(t, "rebalance-a", topic, groupID, strategy, trackerA)
	defer consumerA.Close()
	startRebalanceConsumer(t, consumerA, results)
	waitOwned(t, "A owns both partitions", func() bool {
		return slices.Equal(trackerA.ownedPartitions(), []int32{0, 1})
	})

	trackerB := newPartitionTracker()
	consumerB := newRebalanceConsumer(t, "rebalance-b", topic, groupID, strategy, trackerB)
	defer consumerB.Close()
	startRebalanceConsumer(t, consumerB, results)
	waitOwned(t, "A and B own one partition each", func() bool {
		a, b := trackerA.ownedPartitions(), trackerB.ownedPartitions()
		return len(a) == 1 && len(b) == 1 && a[0] != b[0]
	})

	// each message is handled by the consumer that owns its partition, with a poll time in ctx
	ownerOf := map[int32]string{trackerA.ownedPartitions()[0]: consumerA.Name, trackerB.ownedPartitions()[0]: consumerB.Name}
	values := writeToEachPartition(t, topic, partitions, "split")
	got := receiveValues(t, results, values)
	for partition, value := range values {
		assert.Equal(t, ownerOf[partition], got[value].consumer, "owner of partition %d", partition)
		assert.True(t, got[value].polledOK, "poll time in ctx")
	}

	// B leaves: A gets both partitions back and consumes them
	require.NoError(t, consumerB.Close())
	waitOwned(t, "A owns both partitions again", func() bool {
		return slices.Equal(trackerA.ownedPartitions(), []int32{0, 1})
	})
	assert.Empty(t, trackerB.ownedPartitions(), "B revoked its partition while closing")

	values = writeToEachPartition(t, topic, partitions, "after-leave")
	got = receiveValues(t, results, values)
	for _, value := range values {
		assert.Equal(t, consumerA.Name, got[value].consumer)
	}

	require.NoError(t, consumerA.Close())
	assert.Empty(t, trackerA.ownedPartitions(), "A revoked its partitions while closing")
	return trackerA.revokedSets()
}

// Default (eager) protocol: the hooks see every assign and revoke, and a rebalance revokes all partitions.
func Test_BaseKKConsumer_RebalanceHooks_Eager(t *testing.T) {
	revokes := runRebalanceScenario(t, "")

	log.Printf("revokes: %v", revokes)

	// when B joined, A gave up both partitions before getting one back
	require.NotEmpty(t, revokes)
	assert.Equal(t, 3, len(revokes))
	assert.Equal(t, []int32{0, 1}, revokes[0]) // 1st time when B joined
	assert.Equal(t, 1, len(revokes[1]))        // 2st time when B left
	assert.Equal(t, []int32{0, 1}, revokes[2]) // last time when A closed
}

// Cooperative-sticky: partitions are assigned and revoked incrementally, so A keeps one partition
// while B joins and only the moved partition is revoked.
func Test_BaseKKConsumer_RebalanceHooks_CooperativeSticky(t *testing.T) {
	revokes := runRebalanceScenario(t, "cooperative-sticky")

	log.Printf("revokes: %v", revokes)

	require.NotEmpty(t, revokes)
	assert.Equal(t, 2, len(revokes))
	assert.Len(t, revokes[0], 1, "only the moved partition is revoked when B joins") //1st time when B joined
	//B left not caused revoke, because strategy is cooperative-sticky
	assert.Equal(t, []int32{0, 1}, revokes[1]) //last time when A closed
}

// A message retried by the consume loop keeps the poll time of its first attempt.
func Test_BaseKKConsumer_PolledAt_SameOnRetries(t *testing.T) {
	kafkaURL := mustKafkaURL(t)
	topic := randomTopic("polled-at")

	require.NoError(t, admin.CreateKafkaTopics(kafkaURL, admin.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}))
	defer func() {
		_ = admin.DeleteKafkaTopics(kafkaURL, topic)
	}()

	c, err := consumer.NewBaseKKConsumer("polled-at", consumer.KafkaGroupConsumerConfig{
		BootstrapServers: strings.Split(kafkaURL, ","),
		Topics:           []string{topic},
		GroupID:          randomGroup("polled-at-group"),
		AutoOffsetReset:  "earliest",
	}, consumer.KafkaConsumerRetryPolicy{
		MaxFailure:    5,
		BackoffBase:   20 * time.Millisecond,
		BackoffMax:    20 * time.Millisecond,
		BackoffFactor: 1,
	}, consumer.KafkaConsumerDLTPolicy{})
	require.NoError(t, err)
	defer c.Close()

	var mu sync.Mutex
	seen := []time.Time{}
	done := make(chan struct{})
	err = c.StartConsume(func(ctx context.Context, message *kafka.Message) error {
		polledAt, ok := consumer.PolledAt(ctx)
		assert.True(t, ok)
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, polledAt)
		if len(seen) < 3 {
			return kkerror.Retryable(assert.AnError)
		}
		close(done)
		return nil
	}, func() {}, func() {})
	require.NoError(t, err)

	require.NoError(t, producer.GetKafkaWriter().WriteMessages(context.Background(), producer.Message{Topic: topic, Value: []byte("v")}))

	select {
	case <-done:
	case <-time.After(rebalanceWaitTimeout):
		t.Fatal("timed out waiting for the third attempt")
	}

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, seen, 3)
	assert.True(t, seen[0].Equal(seen[1]) && seen[1].Equal(seen[2]), "poll time is the same on every attempt")
	assert.False(t, seen[0].IsZero())
}
