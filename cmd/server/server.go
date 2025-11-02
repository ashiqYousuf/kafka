package server

import (
	"context"

	"github.com/ashiqYousuf/kafka/internal/config"
	"github.com/ashiqYousuf/kafka/internal/kafka/admin"
	"github.com/ashiqYousuf/kafka/internal/kafka/producer"
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

	if err := producer.InitProducerClient(ctx); err != nil {
		logger.Logger(ctx).Fatal("kafka producer init error", zap.Error(err))
	}
}
