package kkErrors

import "errors"

var (
	KKConsumerNotInitErr = errors.New("kafka consumer is not initialized")
)
