## go-confluent-kafka-reliability-patterns v0.0.7

This release improves consumer shutdown safety for CGO-backed Kafka polling.

### Highlights
- Improved `BaseKKConsumer.Close()` sequencing to wait for consume-loop termination after context cancellation
- Added bounded wait (up to 30 seconds) on `GetConsumeDoneChan()` before reader close
- Reduced risk of CGO-level crash scenarios when `Reader.Poll()` is still active during shutdown

### Behavior Notes
- `Close()` now attempts graceful consume-loop termination before destroying the reader
- On slow or blocked poll loops, shutdown waits up to 30s before proceeding

### Docs
- Updated `README.md` to:
  - mark `v0.0.7` as latest
  - describe the consumer shutdown-safety improvement in "What's New In v0.0.7"

### Full Changelog
- Compare: https://github.com/saivnct/go-confluent-kafka-reliability-patterns/compare/v0.0.6...v0.0.7

