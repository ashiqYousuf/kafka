package producer

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/ashiqYousuf/kafka/internal/config"
	"github.com/ashiqYousuf/kafka/internal/kafka/schema"
	"github.com/ashiqYousuf/kafka/pkg/constants"
	"github.com/ashiqYousuf/kafka/pkg/logger"
	"go.uber.org/zap"
)

/*
	1. SyncProducer: Sends operation → waits for Kafka ack
	2. AsyncProducer: Queues → background works send later
	3. async model wins for high throughput.
	4. You don’t directly communicate with Kafka — you enqueue messages to Sarama’s internal system.
	5. Sarama’s goroutines handle networking, batching, retries.
	6. You must consume Successes/Errors to keep flow non-blocking.
	7. Successes() is a channel of successful deliveries
	8. Errors() is a channel of failed deliveries
	9. If you don’t read from them, they fill up
	10. This is why we spin 2 goroutines — one to drain Successes, one for Errors
*/

var (
	producerClient *Producer
	once           sync.Once
)

type IKafkaProducer interface {
	Close() error
	Input() chan<- *sarama.ProducerMessage
	Successes() <-chan *sarama.ProducerMessage
	Errors() <-chan *sarama.ProducerError
}

type Producer struct {
	client IKafkaProducer
}

func InitProducerClient(ctx context.Context) error {
	var clientError error
	once.Do(func() {
		cfg := sarama.NewConfig()
		sv, err := sarama.ParseKafkaVersion(config.GetConfig().KafkaConfig.KafkaVersion)
		if err != nil {
			logger.Logger(ctx).Error("kafka.producer: unable to parse kafka version", zap.Error(err))
			clientError = err
		}

		cfg.Version = sv
		cfg.Producer.RequiredAcks = sarama.WaitForAll        // safest: leader + replicas
		cfg.Producer.Idempotent = true                       // exactly-once semantics
		cfg.Producer.Return.Errors = true                    // handle errors
		cfg.Producer.Partitioner = sarama.NewHashPartitioner // key-based ordering
		cfg.Producer.Compression = sarama.CompressionSnappy
		cfg.Producer.Return.Successes = true

		// batching for throughput
		cfg.Producer.Flush.Bytes = 32 * 1024                 // batch.size 32KB
		cfg.Producer.Flush.Messages = 100                    // or when 100 msgs collected
		cfg.Producer.Flush.Frequency = 10 * time.Millisecond // linger.ms
		cfg.Producer.Retry.Max = 10                          // broker retries
		cfg.Producer.MaxMessageBytes = 1000000               // 1MB
		cfg.Net.MaxOpenRequests = 1                          // control inflight for idempotence
		cfg.Producer.RequiredAcks = sarama.WaitForAll
		cfg.Producer.Retry.Backoff = 100 * time.Millisecond

		client, err := sarama.NewAsyncProducer(config.GetConfig().KafkaConfig.KafkaBrokers, cfg)
		if err != nil {
			logger.Logger(ctx).Error("kafka.producer: unable to init producer", zap.Error(err))
			clientError = err
		}

		producerClient = &Producer{
			client: client,
		}

		go producerClient.handleSuccesses()
		go producerClient.handleErrors()

		logger.Logger(ctx).Info("kafka.producer producer client initialized successfully")
	})
	return clientError
}

func GetProducerClient() *Producer {
	if producerClient == nil {
		return &Producer{}
	}
	return producerClient
}

// SetProducerClient used for testing only
func SetProducerClient(mockClient IKafkaProducer) {
	producerClient = &Producer{
		client: mockClient,
	}
}

// Send is for sending the raw bytes
func (p *Producer) Send(ctx context.Context, topic, key string, value []byte) {
	select {
	case p.client.Input() <- &sarama.ProducerMessage{ // async send: that operation only queues the message — not sends it to Kafka
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(value),
	}:
	case <-ctx.Done():
		logger.Logger(ctx).Warn("context cancelled, dropping message",
			zap.String(constants.TOPIC_NAME, topic),
			zap.String(constants.MSG_KEY, key),
			zap.Any(constants.MSG_VALUE, string(value)),
		)
	}
}

// SendAvro serializes `native` using the provided Avro schema string,
// registers or gets schema id and enqueues the resulting bytes to Sarama.
func (p *Producer) SendAvro(ctx context.Context, topic, key string, native map[string]interface{}, schemaStr string) {
	subject := fmt.Sprintf("%s-value", topic)
	payload, err := schema.GetSchemaRegistryClient().AvroSerialize(subject, schemaStr, native)
	if err != nil {
		logger.Logger(ctx).Error("avro serialization failed", zap.Error(err),
			zap.String(constants.TOPIC_NAME, topic),
			zap.String(constants.MSG_KEY, key),
		)
		// Emit metrics or send to DLQ
		return
	}

	logger.Logger(ctx).Info(
		"LOGGING AVRO ENCODED PAYLOAD FOR TESTING",
		zap.Any("payload", payload),
	)

	// publish the serialized bytes
	select {
	case p.client.Input() <- &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(payload),
	}:
	case <-ctx.Done():
		logger.Logger(ctx).Warn("context cancelled, dropping avro message",
			zap.String(constants.TOPIC_NAME, topic),
			zap.String(constants.MSG_KEY, key),
			zap.Any("avro encoded value", string(payload)),
		)
	}
}

func (p *Producer) Close() error {
	return p.client.Close()
}

func (p *Producer) handleErrors() {
	for producerErr := range p.client.Errors() {
		key, value := p.extractKeyValueFromProducerMessage(producerErr.Msg)
		// Try extract schema id if message looks like SR wire format
		var schemaID int
		if producerErr.Msg != nil && len(value) > 1+schema.SchemaIDSize {
			b := make([]byte, 4)
			copy(b, value[1:1+schema.SchemaIDSize])
			schemaID = int(binary.BigEndian.Uint32(b))
		}
		logger.Logger(nil).Error(
			"message send failure",
			zap.String(constants.TOPIC_NAME, producerErr.Msg.Topic),
			zap.String(constants.MSG_KEY, string(key)),
			zap.String(constants.MSG_VALUE, string(value)),
			zap.Int("schema_id", schemaID),
			zap.Error(producerErr.Err),
		)
		// Emit metrics or use DLQ
	}
}

func (p *Producer) handleSuccesses() {
	for _ = range p.client.Successes() {
		logger.Logger(nil).Info("message send successfully :)")
	}
}

func (p *Producer) extractKeyValueFromProducerMessage(msg *sarama.ProducerMessage) (key []byte, value []byte) {
	if msg.Key != nil {
		if k, err := msg.Key.Encode(); err == nil {
			key = k
		}
	}

	if msg.Value != nil {
		if v, err := msg.Value.Encode(); err == nil {
			value = v
		}
	}
	return
}
