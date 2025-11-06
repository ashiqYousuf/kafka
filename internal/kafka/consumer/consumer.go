package consumer

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/ashiqYousuf/kafka/internal/config"
	"github.com/ashiqYousuf/kafka/internal/kafka/schema"
	"github.com/ashiqYousuf/kafka/pkg/constants"
	"github.com/ashiqYousuf/kafka/pkg/logger"
	"github.com/ashiqYousuf/kafka/pkg/utils"
	"go.uber.org/zap"
)

var (
	consumerClient *Consumer
	once           sync.Once
)

type IKafkaConsumer interface {
	Close() error
	Consume(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error
	Errors() <-chan error
}

type Consumer struct {
	client IKafkaConsumer
	group  string
	topics []string
}

// consumerGroupHandler implements sarama.ConsumerGroupHandler
type consumerGroupHandler struct {
	// bussiness logic
	processMessage func(ctx context.Context, msg *sarama.ConsumerMessage) error
}

func InitConsumer(ctx context.Context, group string, topics []string) error {
	var clientError error

	once.Do(func() {
		cfg := sarama.NewConfig()
		version, err := sarama.ParseKafkaVersion(config.GetConfig().KafkaConfig.KafkaVersion)
		if err != nil {
			logger.Logger(ctx).Error("kafka.consumer: unable to parse kafka version", zap.Error(err))
			clientError = err
			return
		}

		cfg.Version = version
		// tells Sarama to return consumer errors on a channel instead of swallowing them
		cfg.Consumer.Return.Errors = true
		// If this consumer group has no saved offset, it will start reading from the latest messages
		// sarama.OffsetNewest for real-time processing (only new data)
		// sarama.OffsetOldest for batch/replay processing: start from earliest only if no committed offsets exist
		cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
		// Range strategy = each consumer gets a contiguous range of partitions (other values:- Round Robin, Sticky)
		cfg.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRange()
		// If a consumer doesn’t send heartbeat within this time → Kafka thinks it’s dead → triggers a rebalance
		cfg.Consumer.Group.Session.Timeout = 30 * time.Second
		// How often this consumer sends a heartbeat to the group coordinator.
		cfg.Consumer.Group.Heartbeat.Interval = 3 * time.Second
		// If a rebalance fails, retry up to 5 times before giving up
		cfg.Consumer.Group.Rebalance.Retry.Max = 5
		// Wait 5 seconds between each rebalance retry
		cfg.Consumer.Group.Rebalance.Retry.Backoff = 5 * time.Second

		// sarama.ConsumerGroup → handles rebalances, partition assignment, and offset commits automatically
		client, err := sarama.NewConsumerGroup(config.GetConfig().KafkaConfig.KafkaBrokers, group, cfg)
		if err != nil {
			logger.Logger(ctx).Error("kafka.consumer: unable to init consumer group", zap.Error(err))
			clientError = err
			return
		}

		consumerClient = &Consumer{
			client: client,
			group:  group,
			topics: topics,
		}

		logger.Logger(ctx).Info("kafka.consumer: consumer group client initialized successfully",
			zap.String("group_id", group),
			zap.Strings("topics", topics),
		)
	})

	return clientError
}

// GetConsumerGroupClient returns the initialized consumer group client
func GetConsumerGroupClient() *Consumer {
	if consumerClient == nil {
		return &Consumer{}
	}
	return consumerClient
}

// SetConsumerGroupClient used for testing
func SetConsumerGroupClient(mockClient IKafkaConsumer) {
	consumerClient = &Consumer{
		client: mockClient,
	}
}

// Start begins consuming messages using the given message handler
func (c *Consumer) Start(ctx context.Context, handler func(ctx context.Context, msg *sarama.ConsumerMessage) error) {
	h := &consumerGroupHandler{
		processMessage: handler,
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()

		for {
			/*
				Main Loop (Coordinator)
				Each Consume() call runs a “rebalance cycle”:
				It sets up a new session (Setup())
				Consumes messages (ConsumeClaim())
				Cleans up when done (Cleanup())
			*/
			if err := c.client.Consume(ctx, c.topics, h); err != nil {
				logger.Logger(ctx).Error("kafka.consumer: error during consuming", zap.Error(err))
				time.Sleep(5 * time.Second) // small backoff on error
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	// 🟣 Start draining async internal errors
	// catches background errors like connection drops, rebalance failures
	go func() {
		for err := range c.client.Errors() {
			logger.Logger(ctx).Error("kafka.consumer: async internal error", zap.Error(err))
		}
	}()

	<-ctx.Done()
	logger.Logger(ctx).Info("kafka.consumer: shutting down gracefully")
	wg.Wait()

	if err := c.Close(); err != nil {
		logger.Logger(ctx).Error("kafka.consumer: error closing consumer group", zap.Error(err))
	}

}

func (c *Consumer) Close() error {
	return c.client.Close()
}

// Setup called once per rebalance cycle, right before messages start coming in.
func (h *consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error {
	logger.Logger(nil).Info("kafka.consumer: session setup complete")
	return nil
}

// Cleanup called after the consumer is done consuming (like on shutdown or rebalance).
func (h *consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	logger.Logger(nil).Info("kafka.consumer: session cleanup complete")
	return nil
}

func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// claim.Messages() is an infinite stream of new messages
	// Each msg is one Kafka record (key, value, topic, partition, offset).
	// claim = the set of partitions that Kafka assigned to this consumer during the current rebalance.
	for msg := range claim.Messages() {
		ctx := logger.WithRqId(context.Background(), utils.GenReqId())
		schemaID, native, err := schema.GetSchemaRegistryClient().AvroDeserialize(msg.Value)
		if err != nil {
			logger.Logger(ctx).Error("kafka.consumer: avro deserialization failed",
				zap.String(constants.TOPIC_NAME, msg.Topic),
				zap.Int32("partition", msg.Partition),
				zap.Int64("offset", msg.Offset),
				zap.String("key", string(msg.Key)),
				zap.ByteString("value", msg.Value), // raw binary
				zap.Int("schemaID", schemaID),
				zap.Error(err),
			)
			continue
		}

		jsonBytes, _ := json.Marshal(native)
		msg.Value = jsonBytes
		// Business handler
		err = h.processMessage(ctx, msg)
		if err != nil {
			logger.Logger(ctx).Error("kafka.consumer: message processing failed",
				zap.String(constants.TOPIC_NAME, msg.Topic),
				zap.Int32("partition", msg.Partition),
				zap.Int64("offset", msg.Offset),
				zap.String("key", string(msg.Key)),
				zap.Any("value", native),
				zap.Error(err),
			)
			continue
		}

		session.MarkMessage(msg, "") // mark offset committed

		logger.Logger(ctx).Info("kafka.consumer: message processed successfully",
			zap.String("topic", msg.Topic),
			zap.String("timestamp", msg.Timestamp.Local().String()),
			zap.Int32("partition", msg.Partition),
			zap.Int64("offset", msg.Offset),
			zap.String("key", string(msg.Key)),
			zap.Any("value", native),
		)
	}
	return nil
}
