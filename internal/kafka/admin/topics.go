package admin

import "github.com/ashiqYousuf/kafka/internal/config"

type Topic struct {
	Name              string
	NumPartitions     int
	ReplicationFactor int
	ExtraParams       map[string]string
}

func GetUserTopic() *Topic {
	return &Topic{
		Name:              config.GetConfig().KafkaConfig.KafkaUserTopic.TopicName,
		NumPartitions:     int(config.GetConfig().KafkaConfig.KafkaUserTopic.NumPartitions),
		ReplicationFactor: int(config.GetConfig().KafkaConfig.KafkaUserTopic.DefaultRF),
		ExtraParams:       config.GetConfig().KafkaConfig.KafkaUserTopic.ExtraParams,
	}
}
