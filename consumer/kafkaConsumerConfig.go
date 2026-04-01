package consumer

import (
	"fmt"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/saivnct/go-confluent-kafka-reliability-patterns/util"
)

type KafkaGroupConsumerConfig struct {
	BootstrapServers []string
	Topics           []string
	GroupID          string
	AutoOffsetReset  string
	EnableAutoCommit bool
	IsolationLevel   string
	AdditionalConfig map[string]interface{}
}

func DefaultKafkaGroupConsumerConfig() KafkaGroupConsumerConfig {
	return KafkaGroupConsumerConfig{
		AutoOffsetReset:  "earliest",
		EnableAutoCommit: false,
		IsolationLevel:   "read_committed",
	}
}

func (c KafkaGroupConsumerConfig) normalize() KafkaGroupConsumerConfig {
	defaults := DefaultKafkaGroupConsumerConfig()

	if len(c.BootstrapServers) == 0 {
		kafkaURL := util.EnvString("KAFKA_URL", "")
		if kafkaURL != "" {
			c.BootstrapServers = util.SplitComma(kafkaURL)
		}
	}
	if c.AutoOffsetReset == "" {
		c.AutoOffsetReset = defaults.AutoOffsetReset
	}
	if c.IsolationLevel == "" {
		c.IsolationLevel = defaults.IsolationLevel
	}

	return c
}

func NewKafkaGroupConsumer(cfg KafkaGroupConsumerConfig) (*kafka.Consumer, error) {
	cfg = cfg.normalize()
	if len(cfg.BootstrapServers) == 0 {
		return nil, fmt.Errorf("bootstrap servers are required")
	}
	if cfg.GroupID == "" {
		return nil, fmt.Errorf("group id is required")
	}
	if len(cfg.Topics) == 0 {
		return nil, fmt.Errorf("topics are required")
	}

	consumerConfig := kafka.ConfigMap{
		"bootstrap.servers":               strings.Join(cfg.BootstrapServers, ","),
		"group.id":                        cfg.GroupID,
		"auto.offset.reset":               cfg.AutoOffsetReset,
		"enable.auto.commit":              cfg.EnableAutoCommit,
		"enable.auto.offset.store":        false,
		"isolation.level":                 cfg.IsolationLevel,
		"go.application.rebalance.enable": true,
	}

	for key, value := range cfg.AdditionalConfig {
		if err := consumerConfig.SetKey(key, value); err != nil {
			return nil, fmt.Errorf("invalid consumer config %s: %w", key, err)
		}
	}

	c, err := kafka.NewConsumer(&consumerConfig)
	if err != nil {
		return nil, err
	}

	if err := c.SubscribeTopics(cfg.Topics, nil); err != nil {
		c.Close()
		return nil, err
	}

	return c, nil
}

func NewBaseKKConsumer(name string, cfg KafkaGroupConsumerConfig, retryPolicy KafkaConsumerRetryPolicy) (*BaseKKConsumer, error) {
	c, err := NewKafkaGroupConsumer(cfg)
	if err != nil {
		return nil, err
	}

	return &BaseKKConsumer{
		Name:        name,
		Reader:      c,
		Topics:      append([]string(nil), cfg.Topics...),
		GroupID:     cfg.GroupID,
		RetryPolicy: retryPolicy,
		DLTPolicy:   KafkaConsumerDLTPolicy{},
	}, nil
}
