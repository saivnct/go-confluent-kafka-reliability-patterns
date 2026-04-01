package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"github.com/saivnct/kafka-cf-reliablity/util"
	"github.com/stretchr/testify/assert"
)

func mustKafkaURL(t *testing.T) string {
	kafkaURL := util.EnvString("KAFKA_URL", "")
	assert.NotEmpty(t, kafkaURL)
	return kafkaURL
}

func randomTopic(prefix string) string {
	return fmt.Sprintf("%s-%s", prefix, uuid.NewString())
}

func randomGroup(prefix string) string {
	return fmt.Sprintf("%s-%s", prefix, uuid.NewString())
}

func pollMessage(c *kafka.Consumer, timeout time.Duration) (*kafka.Message, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		event := c.Poll(200)
		if event == nil {
			continue
		}
		switch e := event.(type) {
		case *kafka.Message:
			return e, nil
		case kafka.Error:
			if e.Code() == kafka.ErrTimedOut {
				continue
			}
			return nil, e
		}
	}
	return nil, context.DeadlineExceeded
}

func headerValue(message *kafka.Message, key string) string {
	if message == nil {
		return ""
	}
	for _, h := range message.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
