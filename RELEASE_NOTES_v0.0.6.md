## go-confluent-kafka-reliability-patterns v0.0.6

This release improves producer reliability controls and adds consumer logging configurability.

### Highlights
- Added explicit dedicated-partition routing for producer messages via `Message.UseDedicatedPartition` + `Message.Partition`
- Improved async produce safety by cloning message batches inside `WriteMessagesAsync`
- Added `BaseKKConsumer.IsLogEnable` to toggle internal consumer loop logs
- Improved producer local queue backpressure behavior when enqueue hits `kafka.ErrQueueFull`

### Behavior Notes
- `Message.Partition` is now only applied when `Message.UseDedicatedPartition` is `true`
- If you previously set `Message.Partition` alone, set `UseDedicatedPartition: true` to preserve dedicated partition routing behavior

### Docs
- Updated `README.md` to:
  - mark `v0.0.6` as latest
  - document dedicated partition routing and consumer log toggle
  - include a small "What\'s New In v0.0.6" section

### Full Changelog
- Compare: https://github.com/saivnct/go-confluent-kafka-reliability-patterns/compare/v0.0.5...v0.0.6

