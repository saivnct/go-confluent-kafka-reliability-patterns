package test

import (
	"context"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/joho/godotenv"
	"github.com/saivnct/go-confluent-kafka-reliability-patterns/admin"
	"github.com/saivnct/go-confluent-kafka-reliability-patterns/producer"
	"github.com/saivnct/go-confluent-kafka-reliability-patterns/util"
	"github.com/testcontainers/testcontainers-go"
	kafkaTestContainer "github.com/testcontainers/testcontainers-go/modules/kafka"
	"go.uber.org/zap"
)

var kafkaContainer *kafkaTestContainer.KafkaContainer

func TestMain(m *testing.M) {
	InitTestEnv()
	code := m.Run()
	CloseTestEnv()
	os.Exit(code)
}

func InitTestEnv() {
	err := godotenv.Load("./.env")
	if err != nil {
		log.Fatalf("Failed to load env: %v", err)
	}

	kafkaContainerCtx := context.Background()
	kafkaContainer, err = kafkaTestContainer.Run(
		kafkaContainerCtx,
		"confluentinc/confluent-local:7.5.0",
		kafkaTestContainer.WithClusterID("test-cluster-cf"),
	)
	if err != nil {
		log.Fatalf("Failed to init kafkaContainer: %v", err)
	}

	brokers, err := kafkaContainer.Brokers(kafkaContainerCtx)
	if err != nil {
		log.Fatalf("Failed to get kafka brokers: %v", err)
	}

	brokersURL := strings.Join(brokers, ",")
	if brokersURL == "" {
		log.Fatal("Failed to init kafka brokers url")
	}

	if err = os.Setenv("KAFKA_URL", brokersURL); err != nil {
		log.Fatalf("Failed to set KAFKA_URL: %v", err)
	}

	kkHealthCheckTopic := util.EnvString("KAFKA_HEALTH_CHECK_TOPIC", "")
	if kkHealthCheckTopic != "" {
		if err := admin.CreateKafkaTopics(brokersURL, admin.TopicConfig{
			Topic:             kkHealthCheckTopic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		}); err != nil {
			log.Fatalf("Failed to create health-check topic: %v", err)
		}
	}

	producer.GetKafkaWriter()
	log.Println("Connected to Kafka successfully")
}

func CloseTestEnv() {
	if writer := producer.GetKafkaWriter(); writer != nil {
		log.Println("Closing producer")
		_ = writer.Close()
	}

	if kafkaContainer != nil {
		if err := testcontainers.TerminateContainer(kafkaContainer); err != nil {
			log.Println("Failed to terminate kafka container", zap.Error(err))
		}
	}

	log.Println("✅ Close test environment")
}
