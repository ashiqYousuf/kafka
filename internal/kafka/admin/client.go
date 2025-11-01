package admin

import (
	"context"
	"errors"
	"sync"

	"github.com/IBM/sarama"
	"github.com/ashiqYousuf/kafka/internal/config"
	"github.com/ashiqYousuf/kafka/pkg/constants"
	"github.com/ashiqYousuf/kafka/pkg/logger"
	"go.uber.org/zap"
)

var (
	adminClient *KafkaAdmin
	once        sync.Once
)

type IKafkaAdmin interface {
	CreateTopic(topic string, detail *sarama.TopicDetail, validateOnly bool) error
	ListTopics() (map[string]sarama.TopicDetail, error)
	// DescribeTopics(topics []string) (metadata []*sarama.TopicMetadata, err error)
	// DeleteTopic(topic string) error
	Close() error
}

type KafkaAdmin struct {
	client IKafkaAdmin
}

func GetKafkaAdminClient() *KafkaAdmin {
	return adminClient
}

// SetKafkaAdminClient to be used for testing only
func SetKafkaAdminClient(mockClient IKafkaAdmin) {
	// mockClient will implement all methods of IKafkaAdmin
	adminClient = &KafkaAdmin{
		client: mockClient,
	}
}

func InitKafkaAdminClient(ctx context.Context) error {
	var clientError error
	once.Do(func() {
		sv, err := sarama.ParseKafkaVersion(config.GetConfig().KafkaConfig.KafkaVersion)
		if err != nil {
			logger.Logger(ctx).Error("kafka.admin: unable to parse kafka version", zap.Error(err))
			clientError = err
			return
		}

		saramaCfg := sarama.NewConfig()
		saramaCfg.Version = sv
		saramaCfg.Admin.Timeout = config.GetConfig().KafkaConfig.AdminTimeout
		saramaCfg.Metadata.Retry.Max = config.GetConfig().KafkaConfig.AdminRetryCount

		client, err := sarama.NewClusterAdmin(config.GetConfig().KafkaConfig.KafkaBrokers, saramaCfg)
		if err != nil {
			logger.Logger(ctx).Error("kafka.admin: unable to init kafka cluster admin", zap.Error(err))
			clientError = err
			return
		}
		adminClient = &KafkaAdmin{
			client: client, // client (sarama.ClusterAdmin) already has implements IKafkaAdmin
		}
		logger.Logger(ctx).Info("kafka.admin kafka admin client initialized successfully")
	})

	return clientError
}

func (a *KafkaAdmin) CreateTopic(ctx context.Context, topic *Topic) error {
	topicDetail := a.buildTopicDetail(topic)
	err := a.client.CreateTopic(topic.Name, topicDetail, false)
	if err != nil {
		if errors.Is(err, sarama.ErrTopicAlreadyExists) {
			logger.Logger(ctx).Warn("kafka.admin: topic already exists")
			return nil
		}
		logger.Logger(ctx).Error(
			"kafka.admin: unable to create topic",
			zap.String(constants.TOPIC_NAME, topic.Name),
			zap.Error(err),
		)
		return err
	}

	logger.Logger(ctx).Info("kafka.admin: topic created successfully", zap.String(constants.TOPIC_NAME, topic.Name))
	return nil
}

func (a *KafkaAdmin) ListTopics(ctx context.Context) (map[string]sarama.TopicDetail, error) {
	return a.client.ListTopics()
}

func (a *KafkaAdmin) buildTopicDetail(topic *Topic) *sarama.TopicDetail {
	cfgEntries := map[string]*string{}
	for k, v := range topic.ExtraParams {
		cfgEntries[k] = ptr(v)
	}

	return &sarama.TopicDetail{
		NumPartitions:     int32(topic.NumPartitions),
		ReplicationFactor: int16(topic.ReplicationFactor),
		ConfigEntries:     cfgEntries,
	}
}

func ptr(str string) *string {
	return &str
}

// Close helps to release sockets & resources
func (a *KafkaAdmin) Close() error {
	if a == nil || a.client == nil {
		return nil
	}
	return a.client.Close()
}
