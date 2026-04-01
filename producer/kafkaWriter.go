package producer

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/saivnct/kafka-cf-reliablity/util"
	"go.uber.org/zap"
)

type KafkaWriter struct {
	producer             *kafka.Producer
	deliveryTimeout      time.Duration
	idempotencyHeaderKey string
}

type KafkaProducerConfig struct {
	BootstrapServers     []string
	ClientID             string
	EnableIdempotence    string //true or false, fallback to true
	RequiredAcks         string
	MaxInFlight          int
	Retries              int
	CompressionType      string
	Linger               time.Duration
	MessageTimeout       time.Duration
	DeliveryTimeout      time.Duration
	IdempotencyHeaderKey string
	AdditionalConfig     map[string]interface{}
}

func DefaultKafkaProducerConfig() KafkaProducerConfig {
	return KafkaProducerConfig{
		EnableIdempotence:    "true",
		RequiredAcks:         "all",
		MaxInFlight:          5,
		Retries:              1_000_000,
		CompressionType:      "snappy",
		Linger:               10 * time.Millisecond,
		MessageTimeout:       30 * time.Second,
		DeliveryTimeout:      30 * time.Second,
		IdempotencyHeaderKey: DefaultIdempotencyHeaderKey,
	}
}
func (c KafkaProducerConfig) isIdempotenceEnabled() bool {
	return c.EnableIdempotence == "true"
}

func (c KafkaProducerConfig) normalize() KafkaProducerConfig {
	defaults := DefaultKafkaProducerConfig()

	if len(c.BootstrapServers) == 0 {
		brokers := util.EnvString("KAFKA_URL", "")
		if brokers != "" {
			c.BootstrapServers = util.SplitComma(brokers)
		}
	}

	if len(c.EnableIdempotence) == 0 || (c.EnableIdempotence != "true" && c.EnableIdempotence != "false") {
		c.EnableIdempotence = defaults.EnableIdempotence
	}
	if c.RequiredAcks == "" {
		c.RequiredAcks = defaults.RequiredAcks
	}
	if c.MaxInFlight <= 0 {
		c.MaxInFlight = defaults.MaxInFlight
	}
	if c.Retries <= 0 {
		c.Retries = defaults.Retries
	}
	if c.CompressionType == "" {
		c.CompressionType = defaults.CompressionType
	}
	if c.Linger <= 0 {
		c.Linger = defaults.Linger
	}
	if c.MessageTimeout <= 0 {
		c.MessageTimeout = defaults.MessageTimeout
	}
	if c.DeliveryTimeout <= 0 {
		c.DeliveryTimeout = defaults.DeliveryTimeout
	}
	if c.IdempotencyHeaderKey == "" {
		c.IdempotencyHeaderKey = defaults.IdempotencyHeaderKey
	}

	return c
}

func NewKafkaWriter(config KafkaProducerConfig) (*KafkaWriter, error) {
	config = config.normalize()
	if len(config.BootstrapServers) == 0 {
		return nil, fmt.Errorf("invalid kafka bootstrap servers")
	}

	cm := kafka.ConfigMap{
		"bootstrap.servers":                     strings.Join(config.BootstrapServers, ","),
		"client.id":                             config.ClientID,
		"enable.idempotence":                    config.isIdempotenceEnabled(),
		"acks":                                  config.RequiredAcks,
		"retries":                               config.Retries,
		"max.in.flight.requests.per.connection": config.MaxInFlight,
		"compression.type":                      config.CompressionType,
		"linger.ms":                             int(config.Linger.Milliseconds()),
		"message.timeout.ms":                    int(config.MessageTimeout.Milliseconds()),
		"go.delivery.reports":                   true,
	}

	if config.ClientID == "" {
		_ = cm.SetKey("client.id", "common-kk-cf-producer")
	}

	for key, value := range config.AdditionalConfig {
		if err := cm.SetKey(key, value); err != nil {
			return nil, fmt.Errorf("invalid producer config %s: %w", key, err)
		}
	}

	producer, err := kafka.NewProducer(&cm)
	if err != nil {
		return nil, err
	}

	writer := &KafkaWriter{
		producer:             producer,
		deliveryTimeout:      config.DeliveryTimeout,
		idempotencyHeaderKey: config.IdempotencyHeaderKey,
	}

	return writer, nil
}

func (w *KafkaWriter) Close() error {
	if w == nil || w.producer == nil {
		return nil
	}

	_ = w.producer.Flush(60_000)
	w.producer.Close()
	w.producer = nil
	return nil
}

func (w *KafkaWriter) RawProducer() *kafka.Producer {
	if w == nil {
		return nil
	}
	return w.producer
}

var singletonKafkaWriter *KafkaWriter
var onceKafkaWriter sync.Once

func GetKafkaWriter() *KafkaWriter {
	onceKafkaWriter.Do(func() {
		kafkaURL := util.EnvString("KAFKA_URL", "")
		if len(kafkaURL) == 0 {
			log.Fatal("invalid kafka url string")
		}

		writer, err := NewKafkaWriter(KafkaProducerConfig{
			BootstrapServers: util.SplitComma(kafkaURL),
		})
		if err != nil {
			log.Fatal("failed to initialize kafka writer", zap.Error(err))
		}

		kkHealthCheckTopic := util.EnvString("KAFKA_HEALTH_CHECK_TOPIC", "")
		if kkHealthCheckTopic != "" {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			err = writer.WriteMessages(ctx, Message{
				Topic: kkHealthCheckTopic,
				Key:   []byte("health"),
				Value: []byte("ping"),
			})
			if err != nil {
				log.Fatal("failed to init kafka writer", zap.Error(err))
			}
		}

		singletonKafkaWriter = writer
	})

	return singletonKafkaWriter
}
