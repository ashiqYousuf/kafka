package service

import (
	"context"
	"encoding/json"

	"github.com/ashiqYousuf/kafka/internal/config"
	"github.com/ashiqYousuf/kafka/internal/dal"
	"github.com/ashiqYousuf/kafka/internal/kafka/producer"
	"github.com/ashiqYousuf/kafka/internal/kafka/schema"
	"github.com/ashiqYousuf/kafka/pkg/logger"
	"go.uber.org/zap"
)

type createUserRequest struct {
	Name     string `json:"name"`
	Address  string `json:"address"`
	Password string `json:"password"`
}

func CreateUser(ctx context.Context, requestBody []byte) ([]byte, error) {
	request := &createUserRequest{}
	if err := json.Unmarshal(requestBody, request); err != nil {
		logger.Logger(ctx).Error("unmarshalling body error", zap.Error(err))
		return nil, err
	}
	user := dal.GetUserDao(request.Name, request.Address, request.Password)
	// producer.GetProducerClient().Send(
	// 	ctx,
	// 	config.GetConfig().KafkaConfig.KafkaUserTopic.TopicName,
	// 	user.ID,
	// 	user.ToBytes(),
	// )
	producer.GetProducerClient().SendAvro(
		ctx,
		config.GetConfig().KafkaConfig.KafkaUserTopic.TopicName,
		user.ID,
		user.ToAvroNative(),
		schema.UserSchema,
	)
	return sendResponse(user), nil
}

func sendResponse(arg any) []byte {
	d, _ := json.Marshal(arg)
	return d
}
