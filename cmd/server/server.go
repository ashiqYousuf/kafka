package server

import (
	"context"

	"github.com/ashiqYousuf/kafka/internal/config"
	"github.com/ashiqYousuf/kafka/internal/kafka/admin"
	"github.com/ashiqYousuf/kafka/internal/kafka/producer"
	"github.com/ashiqYousuf/kafka/pkg/constants"
	"github.com/ashiqYousuf/kafka/pkg/logger"
	"github.com/ashiqYousuf/kafka/pkg/utils"
	"go.uber.org/zap"
)

func Start() {
	ctx := logger.WithRqId(context.Background(), utils.GenReqId())
	logger.LoggerInit()

	if err := admin.InitKafkaAdminClient(ctx); err != nil {
		logger.Logger(ctx).Fatal("kafka init error", zap.Error(err))
	}

	if !config.GetConfig().KafkaConfig.DisableTopicCreation {
		kafkaAdminOps(ctx)
	}

	producer.InitProducerClient(ctx)
	// producer := producer.GetProducerClient()

	// type user struct {
	// 	Id   int32
	// 	Name string
	// 	Addr string
	// }
	// users := []*user{
	// 	{Id: 1, Name: "User1", Addr: "Ok"},
	// 	{Id: 2, Name: "User2", Addr: "Boh"},
	// 	{Id: 3, Name: "User3", Addr: "HT"},
	// }

	// for _, u := range users {
	// 	d, _ := json.Marshal(u)
	// 	producer.Send(ctx, config.GetConfig().KafkaConfig.KafkaUserTopic.TopicName, string(u.Id), d)
	// }
}

func kafkaAdminOps(ctx context.Context) {
	kafkaAdminClient := admin.GetKafkaAdminClient()
	if err := createTopics(ctx, kafkaAdminClient); err != nil {
		logger.Logger(ctx).Fatal("kafka create topic error", zap.Error(err))
	}

	topics, err := kafkaAdminClient.ListTopics(ctx)
	if err != nil {
		logger.Logger(ctx).Fatal("kafka list topic error", zap.Error(err))
	}

	for topic, topicDetail := range topics {
		logger.Logger(ctx).Info(
			"topic details",
			zap.String(constants.TOPIC_NAME, topic),
			zap.Any("topic", topicDetail),
		)
	}
}

func createTopics(ctx context.Context, kafkaAdminClient *admin.KafkaAdminClient) error {
	topics := make([]*admin.Topic, 0)
	topics = append(topics, admin.GetUserTopic())
	for _, topic := range topics {
		err := kafkaAdminClient.CreateTopic(ctx, topic)
		if err != nil {
			logger.Logger(ctx).Error("error creating topic", zap.String(constants.TOPIC_NAME, topic.Name))
			return err
		}
	}

	return nil
}

// deleteUpdateTopic has no use as of now
func deleteUpdateTopic(ctx context.Context, kafkaAdminClient *admin.KafkaAdminClient) {
	// Update topic
	topic := admin.GetUserTopic()
	topic.ExtraParams[constants.MIN_INSYNC_REPLICAS] = "3"
	topic.ExtraParams[constants.CLEANUP_POLICY] = "compact"
	if err := kafkaAdminClient.AlterTopicConfig(ctx, topic); err != nil {
		logger.Logger(ctx).Error("unable to alter topic", zap.Error(err))
	} else {
		logger.Logger(ctx).Info("topic altered successfully")
	}

	// Delete topic
	if err := kafkaAdminClient.DeleteTopic(ctx, admin.GetUserTopic().Name); err != nil {
		logger.Logger(ctx).Error("unable to deletd topic", zap.Error(err))
	} else {
		logger.Logger(ctx).Info("topic deleted successfully")
	}
}
