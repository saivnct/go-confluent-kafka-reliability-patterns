## go-confluent-kafka-reliability-patterns v0.0.9

This release fixes a consumer bug: a message could be skipped, and lost, when its dead-letter write failed.

### Bug Fix
- Before: when the handler gave up on a message and the dead-letter (DLT) write failed, `ProcessKafkaMessageWithRetryAndOptions` returned the error without committing, and the consume loop polled the next message. The commit of the next message also covered the failed one, so the failed message was never handled again and was not in the DLT.
- Now: the DLT write is retried up to `KafkaConsumerDLTPolicy.MaxWriteAttempts` times (default 10) with the backoff of the retry policy. Only the write is retried; the handler is not run again. When every try fails, the message is not committed and the consume loop stops with `kkErrors.KKDLTWriteFailedErr`, so a restart reads the same message again.

### Highlights
- `KafkaConsumerDLTPolicy.MaxWriteAttempts` (`<= 0` means `consumer.DefaultDLTMaxWriteAttempts` = 10)
- `kkErrors.KKDLTWriteFailedErr`: wraps the last DLT write error; a missing DLT writer or topic returns it at once
- `kkErrors.KKCommitFailedErr`: wraps an offset commit error after a handled message; the consume loop logs it and continues at once (no sleep, no second run of the handler), because a later commit covers the offset

### Behavior Notes
- With the default retry policy (500 ms, x2, max 10 s), 10 DLT tries wait about 56 s in total, below the default `max.poll.interval.ms` (5 min). Keep `MaxWriteAttempts` and the backoff below that limit, because `Poll` is not called while the write is retried.
- When the consume loop stops with `KKDLTWriteFailedErr`, the reader is not closed. Watch `GetConsumeDoneChan()` / `ConsumeErr()` and close or restart the consumer (for example, end the process so it is restarted).
- After a context cancel during the DLT retries, the loop stops as before (shutdown), without committing.
- Function signatures do not change.

### Tests
- Unit tests: the DLT write fails then works (the handler runs once), always fails (stops after `MaxWriteAttempts`, no commit), default limit, context canceled during the backoff, missing DLT writer, commit failure
- Integration tests on Kafka: the DLT write fails once (the message reaches the DLT and is not skipped), always fails (the consumer stops, the next message is not handled, nothing is committed). Both fail on v0.0.8.

### Full Changelog
- Compare: https://github.com/saivnct/go-confluent-kafka-reliability-patterns/compare/v0.0.8...v0.0.9
