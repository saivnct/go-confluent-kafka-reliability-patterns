package conn

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type TopicConfig struct {
	Topic             string
	NumPartitions     int
	ReplicationFactor int
	ConfigEntries     map[string]string
}

func CreateKafkaTopics(kafkaURL string, topicConfig ...TopicConfig) error {
	if len(topicConfig) == 0 {
		return nil
	}

	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": kafkaURL,
	})
	if err != nil {
		return err
	}
	defer adminClient.Close()

	specs := make([]kafka.TopicSpecification, 0, len(topicConfig))
	topics := make([]string, 0, len(topicConfig))
	for _, cfg := range topicConfig {
		specs = append(specs, kafka.TopicSpecification{
			Topic:             cfg.Topic,
			NumPartitions:     cfg.NumPartitions,
			ReplicationFactor: cfg.ReplicationFactor,
			Config:            cfg.ConfigEntries,
		})
		topics = append(topics, cfg.Topic)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	results, err := adminClient.CreateTopics(ctx, specs)
	if err != nil {
		return err
	}

	var errs []string
	for _, result := range results {
		if result.Error.Code() == kafka.ErrNoError || result.Error.Code() == kafka.ErrTopicAlreadyExists {
			continue
		}
		errs = append(errs, fmt.Sprintf("topic=%s error=%v", result.Topic, result.Error))
	}
	if len(errs) > 0 {
		return fmt.Errorf("create kafka topics failed: %s", strings.Join(errs, "; "))
	}

	return nil
}

func DeleteKafkaTopics(kafkaURL string, topics ...string) error {
	if len(topics) == 0 {
		return nil
	}

	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": kafkaURL,
	})
	if err != nil {
		return err
	}
	defer adminClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	results, err := adminClient.DeleteTopics(ctx, topics)
	if err != nil {
		return err
	}

	var errs []string
	for _, result := range results {
		if result.Error.Code() == kafka.ErrNoError || result.Error.Code() == kafka.ErrUnknownTopicOrPart {
			continue
		}
		errs = append(errs, fmt.Sprintf("topic=%s error=%v", result.Topic, result.Error))
	}
	if len(errs) > 0 {
		return fmt.Errorf("delete kafka topics failed: %s", strings.Join(errs, "; "))
	}

	return nil
}
