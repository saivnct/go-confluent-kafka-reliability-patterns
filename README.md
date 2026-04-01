# go-confluent-kafka-enhanced

[![Release](https://img.shields.io/github/v/tag/saivnct/go-confluent-kafka-enhanced?label=release)](https://github.com/saivnct/go-confluent-kafka-enhanced/tags)
[![Go Version](https://img.shields.io/badge/go-1.26.0-00ADD8?logo=go)](https://go.dev/)
[![Go Reference](https://pkg.go.dev/badge/github.com/saivnct/kafka-cf-reliablity.svg)](https://pkg.go.dev/github.com/saivnct/kafka-cf-reliablity)
[![License: Unspecified](https://img.shields.io/badge/license-unspecified-lightgrey)](#license)

A production-oriented Go wrapper around Confluent Kafka (`confluent-kafka-go`) focused on **safe producer defaults**, **controlled consumer retries**, and **dead-letter workflows**.

Built for teams that want to move fast without re-implementing Kafka reliability patterns in every service.

## Why This Library

Most Kafka clients solve connectivity, not operating model.

`go-confluent-kafka-enhanced` gives you a consistent baseline for:
- Topic administration with idempotent-friendly behavior
- Idempotent producer defaults and automatic idempotency header injection
- Consumer retry orchestration with exponential backoff
- DLT publishing with rich failure metadata

## Features

### Topic Administration (`conn`)
- `CreateKafkaTopics(...)`
- `DeleteKafkaTopics(...)`
- Handles common idempotent cases:
  - create ignores `topic already exists`
  - delete ignores `unknown topic`

### Producer Wrapper (`producer`)
- `NewKafkaWriter(config)` for explicit construction
- `GetKafkaWriter()` singleton from env (`KAFKA_URL`)
- Idempotent producer defaults:
  - `enable.idempotence=true`
  - `acks=all`
- Auto `x-idempotency-key` header (if missing)
- Multiple write modes:
  - `WriteMessages` (sync with delivery reports)
  - `WriteMessagesAsync`
  - `WriteMessagesFireAndForget`

### Consumer Wrapper (`consumer`)
- `NewKafkaGroupConsumer(config)` for group consumer setup
- `BaseKKConsumer` polling loop with lifecycle hooks
- Retry engine with configurable backoff policy
- Retry/non-retryable error semantics via `kkErrors`
- Optional DLT publishing on terminal or exhausted failures

## Installation

```bash
go get github.com/saivnct/kafka-cf-reliablity
```

## Quick Start

### 1) Produce Messages

```go
package main

import (
    "context"
    "log"

    "github.com/saivnct/kafka-cf-reliablity/producer"
)

func main() {
    writer := producer.GetKafkaWriter() // requires KAFKA_URL

    err := writer.WriteMessages(context.Background(), producer.Message{
        Topic: "orders",
        Key:   []byte("order-1"),
        Value: []byte(`{"status":"created"}`),
    })
    if err != nil {
        log.Fatal(err)
    }
}
```

### 2) Consume With Retry + DLT

```go
package main

import (
    "context"
    "errors"
    "strings"
    "time"

    "github.com/confluentinc/confluent-kafka-go/v2/kafka"

    "github.com/saivnct/kafka-cf-reliablity/consumer"
    kkerror "github.com/saivnct/kafka-cf-reliablity/kkErrors"
    "github.com/saivnct/kafka-cf-reliablity/producer"
)

func main() {
    base, err := consumer.NewBaseKKConsumer(
        "orders-consumer",
        consumer.KafkaGroupConsumerConfig{
            BootstrapServers: strings.Split("localhost:9092", ","),
            Topics:           []string{"orders"},
            GroupID:          "orders-group",
            AutoOffsetReset:  "earliest",
        },
        consumer.KafkaConsumerRetryPolicy{
            MaxFailure:    5,
            BackoffBase:   500 * time.Millisecond,
            BackoffMax:    10 * time.Second,
            BackoffFactor: 2,
        },
    )
    if err != nil {
        panic(err)
    }
    defer base.Close()

    base.DLTPolicy = consumer.KafkaConsumerDLTPolicy{
        Enabled: true,
        Topic:   "orders.DLT", // optional; defaults to <sourceTopic>.DLT
        Writer:  producer.GetKafkaWriter(),
    }

    _ = base.StartConsume(func(ctx context.Context, msg *kafka.Message) error {
        _ = ctx
        _ = msg

        // Example retryable error:
        return kkerror.Retryable(errors.New("temporary downstream error"))

        // Example non-retryable error:
        // return kkerror.NotRetryable(errors.New("invalid payload"))
    }, nil, nil)

    select {}
}
```

## Configuration

### Environment Variables
- `KAFKA_URL`: broker list used by `GetKafkaWriter()` and config fallbacks
- `KAFKA_HEALTH_CHECK_TOPIC` (optional): if set, singleton writer sends a startup ping message

### Producer Defaults
`producer.DefaultKafkaProducerConfig()` uses:
- `EnableIdempotence: "true"`
- `RequiredAcks: "all"`
- `MaxInFlight: 5`
- `Retries: 1000000`
- `CompressionType: "snappy"`
- `Linger: 10ms`
- `MessageTimeout: 30s`
- `DeliveryTimeout: 30s`

### Consumer Defaults
`consumer.DefaultKafkaGroupConsumerConfig()`:
- `AutoOffsetReset: "earliest"`
- `EnableAutoCommit: false`
- `IsolationLevel: "read_committed"`

`consumer.DefaultKafkaConsumerRetryPolicy()`:
- `MaxFailure: 5`
- `BackoffBase: 500ms`
- `BackoffMax: 10s`
- `BackoffFactor: 2`

## Error Semantics

Use `kkErrors` to control retry behavior in handlers:
- `kkErrors.Retryable(err)` -> eligible for retry
- `kkErrors.NotRetryable(err)` -> terminal failure
- unwrapped/plain errors are treated as terminal

## DLT Metadata Headers

On DLT publish, the library enriches headers with:
- `x-dlt-original-topic`
- `x-dlt-original-partition`
- `x-dlt-original-offset`
- `x-dlt-failure-reason`
- `x-dlt-failure-attempts`
- `x-dlt-failed-at`

## Testing

Run unit + integration tests:

```bash
go test ./...
```

Integration tests use `testcontainers-go` and require Docker.

## Versioning

This project follows semantic versioning.

- `v0.x.x`: fast iteration, APIs may evolve
- `v1.x.x`: stable public API guarantees

## Roadmap

- richer observability hooks (structured logs/metrics/tracing)
- configurable retry classifiers
- optional pluggable idempotency store for consumer workflows
- stronger lifecycle/state management for long-running consumers

## Contributing

Issues and PRs are welcome. If you are using this library in production, sharing feedback and edge cases is highly appreciated.

## License

Add your preferred license file (for example `MIT`) before wider public distribution.
