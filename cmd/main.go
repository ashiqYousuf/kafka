package main

import (
	"context"
	"time"

	"github.com/ashiqYousuf/kafka/internal/kafka/admin"
	"github.com/ashiqYousuf/kafka/pkg/constants"
	"github.com/ashiqYousuf/kafka/pkg/logger"
	"go.uber.org/zap"
)

// TODO: Get Func Name code from Code!
func main() {
	ctx := logger.WithRqId(context.Background(), constants.APP_NAME)
	logger.LoggerInit()

	if err := admin.InitKafkaAdminClient(ctx); err != nil {
		logger.Logger(ctx).Fatal("kafka init error", zap.Error(err))
	}

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
		time.Sleep(time.Second * 2)
	}
}

func createTopics(ctx context.Context, kafkaAdminClient *admin.KafkaAdmin) error {
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
