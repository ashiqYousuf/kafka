package producer

import (
	"context"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/ashiqYousuf/kafka/internal/config"
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

func (p *Producer) Close() error {
	return p.client.Close()
}

func (p *Producer) handleErrors() {
	for producerErr := range p.client.Errors() {
		key, value := p.extractKeyValueFromProducerMessage(producerErr.Msg)
		logger.Logger(nil).Error(
			"message delivery failed",
			zap.String(constants.TOPIC_NAME, producerErr.Msg.Topic),
			zap.String(constants.MSG_KEY, key),
			zap.String(constants.MSG_VALUE, value),
			zap.Error(producerErr.Err),
		)
	}
}

func (p *Producer) handleSuccesses() {
	for producerMsg := range p.client.Successes() {
		key, value := p.extractKeyValueFromProducerMessage(producerMsg)
		logger.Logger(nil).Info(
			"message delivery success",
			zap.String(constants.TOPIC_NAME, producerMsg.Topic),
			zap.String(constants.MSG_KEY, key),
			zap.String(constants.MSG_VALUE, value),
			zap.Int64(constants.MSG_OFFSET, producerMsg.Offset),
			zap.Int32(constants.MSG_PARTITION, producerMsg.Partition),
		)
	}
}

func (p *Producer) extractKeyValueFromProducerMessage(msg *sarama.ProducerMessage) (key string, value string) {
	if msg.Key != nil {
		if k, err := msg.Key.Encode(); err == nil {
			key = string(k)
		}
	}

	if msg.Value != nil {
		if v, err := msg.Value.Encode(); err == nil {
			value = string(v)
		}
	}
	return
}
