package kkErrors

// KafkaRetryableError marks an error as retryable by Kafka consumers.
type KafkaRetryableError struct {
	err error
}

func (e *KafkaRetryableError) Error() string {
	if e == nil || e.err == nil {
		return "retryable kafka error"
	}
	return e.err.Error()
}

func (e *KafkaRetryableError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

// Retryable wraps err so consumer retry policies can detect it.
func Retryable(err error) error {
	if err == nil {
		return nil
	}
	if IsRetryable(err) {
		return err
	}
	return &KafkaRetryableError{err: err}
}

// IsRetryable reports whether err is marked as retryable.
func IsRetryable(err error) bool {
	_, ok := err.(*KafkaRetryableError)
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
		_, ok = err.(*KafkaRetryableError)
		if ok {
			return true
		}
	}
	return false
}
