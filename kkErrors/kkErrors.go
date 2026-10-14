package kkErrors

import "errors"

var (
	KKConsumerNotInitErr = errors.New("kafka consumer is not initialized")

	// KKDLTWriteFailedErr wraps the last error of a dead-letter write that failed on every attempt
	// (KafkaConsumerDLTPolicy.MaxWriteAttempts). The message is not committed, and the consume loop stops.
	KKDLTWriteFailedErr = errors.New("kafka dead-letter write failed")

	// KKCommitFailedErr wraps an offset commit error after the message was handled (or written to the
	// dead-letter topic). The consume loop logs it and continues: a later commit covers the message.
	KKCommitFailedErr = errors.New("kafka offset commit failed")
)
