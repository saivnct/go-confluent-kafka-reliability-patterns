package kkErrors

// KafkaNotRetryableError marks an error as not retryable by Kafka consumers.
type KafkaNotRetryableError struct {
	err error
}

func (e *KafkaNotRetryableError) Error() string {
	if e == nil || e.err == nil {
		return "not retryable kafka error"
	}
	return e.err.Error()
}

func (e *KafkaNotRetryableError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

// NotRetryable wraps err so consumer retry policies can detect it.
func NotRetryable(err error) error {
	if err == nil {
		return nil
	}
	if IsNotRetryable(err) {
		return err
	}
	return &KafkaNotRetryableError{err: err}
}

// IsNotRetryable reports whether err is marked as not retryable.
func IsNotRetryable(err error) bool {
	_, ok := err.(*KafkaNotRetryableError)
	if ok {
		return true
	}
	type unwrapper interface{ Unwrap() error }
	for err != nil {
		u, ok := err.(unwrapper)
		if !ok {
			return false
		}
		err = u.Unwrap()
		_, ok = err.(*KafkaNotRetryableError)
		if ok {
			return true
		}
	}
	return false
}
