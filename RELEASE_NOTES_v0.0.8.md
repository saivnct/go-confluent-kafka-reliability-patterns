## go-confluent-kafka-reliability-patterns v0.0.8

This release lets applications react to partition changes and know how long a message has been
handled without polling Kafka. It also adds support for the cooperative rebalance protocol.

### Highlights
- `BaseKKConsumer.RebalanceHooks` (`KafkaRebalanceHooks`):
  - `OnAssigned(partitions)` is called after partitions are assigned
  - `OnRevoked(partitions, lost)` is called before partitions are revoked; `lost` comes from `AssignmentLost()`
- `consumer.PolledAt(ctx)` returns the time the consume loop polled the message being handled; retries of the same message keep this time; `consumer.WithPolledAt(ctx, t)` sets it (for application tests)
- Cooperative rebalance: with `partition.assignment.strategy=cooperative-sticky`, partitions are assigned and revoked with `IncrementalAssign` / `IncrementalUnassign`

### Behavior Notes
- `NewBaseKKConsumer` now subscribes with a rebalance callback. In `Poll` mode the Kafka client never returned `AssignedPartitions` / `RevokedPartitions` to the loop (it applied them itself), so the old cases in the loop were unreachable and are removed. Assignment behavior is unchanged for the default (eager) protocol.
- `NewKafkaGroupConsumer` is unchanged (no callback; the Kafka client applies rebalances).
- Hooks run on the goroutine that calls `Poll`: the consume loop, or `Close` while it leaves the group. Set them before `StartConsume`.
- Switching a running group from eager to cooperative needs all its consumers restarted together (the Kafka client does not allow both protocols in one group).

### Tests
- Unit tests for the rebalance logic (eager, cooperative, lost assignment, assign error, no hooks) and `PolledAt`
- Integration tests with two consumers in one group, for the eager and the cooperative-sticky protocols, and for `PolledAt` across retries
- Test helper `pollMessage` no longer stops on a temporary error (a new topic can be reported as unknown for a short time), which made two producer tests flaky

### Full Changelog
- Compare: https://github.com/saivnct/go-confluent-kafka-reliability-patterns/compare/v0.0.7...v0.0.8
