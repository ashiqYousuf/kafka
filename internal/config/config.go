package config

import (
	"time"

	"github.com/ashiqYousuf/kafka/pkg/constants"
	"github.com/spf13/viper"
)

type Config struct {
	KafkaConfig          *KafkaConfig
	LogConfig            *LogConfig
	ServiceConfig        *ServiceConfig
	SchemaRegistryConfig *SchemaRegistryConfig
}

type ServiceConfig struct {
	HTTPPort int
	Env      string
}

type KafkaConfig struct {
	KafkaBrokers         []string
	KafkaVersion         string
	AdminRetryCount      int           // If fetching metadata (like broker list or topic info) fails, it retries up to 5 times before giving up.
	AdminTimeout         time.Duration // Controls how long admin operations can block before timing out
	AdminRetryBackOff    time.Duration
	DisableTopicCreation bool
	KafkaUserTopic       *KafkaTopic
	KafkaConsumerConfig  *KafkaConsumerConfig
	SchemaRegistryConfig *SchemaRegistryConfig
}

type KafkaTopic struct {
	TopicName     string
	NumPartitions int32
	DefaultRF     int16
	ExtraParams   map[string]string
}

type KafkaConsumerConfig struct {
	GroupID string
	Topics  []string
}

type SchemaRegistryConfig struct {
	URL string
}

type LogConfig struct {
	LogFilePath           string
	LogFileName           string
	LogLevel              string
	LogMaxSize            int
	LogMaxAge             int
	LogMaxBackups         int
	LogCompressionEnabled bool
}

func GetConfig() *Config {
	return &Config{
		KafkaConfig: &KafkaConfig{
			KafkaBrokers:         viper.GetStringSlice("KAFKA_BROKER_URLS"),
			KafkaVersion:         viper.GetString("KAFKA_VERSION"),
			AdminRetryCount:      viper.GetInt("KAFKA_ADMIN_RETRY_COUNT"),
			AdminRetryBackOff:    viper.GetDuration("KAFKA_ADMIN_RETRY_BACKOFF") * time.Millisecond,
			AdminTimeout:         viper.GetDuration("KAFKA_ADMIN_TIMEOUT") * time.Second,
			DisableTopicCreation: viper.GetBool("KAFKA_DISABLE_TOPIC_CREATION"),
			KafkaUserTopic: &KafkaTopic{
				TopicName:     viper.GetString("KAFKA_USER_TOPIC"),
				NumPartitions: viper.GetInt32("KAFKA_USER_NUM_PARTITIONS"),
				DefaultRF:     int16(viper.GetInt32("KAFKA_USER_RF")),
				ExtraParams:   viper.GetStringMapString("KAFKA_USER_EXTRA_PARAMS"),
			},
			KafkaConsumerConfig: &KafkaConsumerConfig{
				GroupID: viper.GetString("KAFKA_CONSUMER_GROUP_ID"),
				Topics:  viper.GetStringSlice("KAFKA_CONSUMER_TOPICS"),
			},
		},
		SchemaRegistryConfig: &SchemaRegistryConfig{
			URL: viper.GetString("SCHEMA_REGISTRY_URL"),
		},
		LogConfig: &LogConfig{
			LogFilePath:           viper.GetString("LOG_FILE_PATH"),
			LogFileName:           viper.GetString("LOG_FILE_NAME"),
			LogLevel:              viper.GetString("LOG_LEVEL"),
			LogMaxSize:            viper.GetInt("LOG_MAX_SIZE"),
			LogMaxAge:             viper.GetInt("LOG_MAX_AGE"),
			LogMaxBackups:         viper.GetInt("LOG_MAX_BACKUPS"),
			LogCompressionEnabled: viper.GetBool("LOG_COMPRESSION_ENABLED"),
		},
		ServiceConfig: &ServiceConfig{
			HTTPPort: viper.GetInt("SERVICE_PORT"),
			Env:      viper.GetString("SERVICE_ENV"),
		},
	}
}

func init() {
	viper.Set("SERVICE_PORT", 8000)
	viper.Set("SERVICE_ENV", "prod")

	viper.Set("KAFKA_BROKER_URLS", []string{"localhost:9092", "localhost:9093", "localhost:9094"})
	viper.Set("KAFKA_VERSION", "3.7.0")
	viper.Set("KAFKA_ADMIN_RETRY_COUNT", 5)
	viper.Set("KAFKA_ADMIN_RETRY_BACKOFF", 500)
	viper.Set("KAFKA_ADMIN_TIMEOUT", 60)
	viper.Set("KAFKA_DISABLE_TOPIC_CREATION", false)
	viper.Set("KAFKA_CONSUMER_GROUP_ID", "consumer-grp-1")
	viper.Set("KAFKA_CONSUMER_TOPICS", []string{"user_created"})

	viper.Set("KAFKA_USER_TOPIC", "user_created")
	viper.Set("KAFKA_USER_NUM_PARTITIONS", 3)
	viper.Set("KAFKA_USER_RF", 3)
	viper.Set("KAFKA_USER_EXTRA_PARAMS", map[string]string{
		constants.MIN_INSYNC_REPLICAS:                "2",           // durability setting
		constants.CLEANUP_POLICY:                     "delete",      // time-based deletion, "compact" value for compact topics (last key survival only)
		constants.SEGMENT_BYTES:                      "1073741824",  // 1 GB per segment
		constants.OUT_OF_SYNC_LEADER_ELECTION_ENABLE: "false",       // no data loss allowed
		constants.COMPRESSION_TYPE:                   "lz4",         // faster, lighter compression
		constants.RETENTION_MS:                       "604800000",   // 7 days
		constants.RETENTION_BYTES:                    "10737418240", // 10 GB per partition
	})

	viper.Set("SCHEMA_REGISTRY_URL", "http://localhost:8081")

	viper.SetDefault("LOG_FILE_PATH", "/var/log/kafka/")
	viper.SetDefault("LOG_FILE_NAME", "app")
	viper.SetDefault("LOG_LEVEL", "INFO")
	viper.SetDefault("LOG_MAX_SIZE", 1) // 1MB
	viper.SetDefault("LOG_MAX_AGE", 30)
	viper.SetDefault("LOG_MAX_BACKUPS", 50)
	viper.SetDefault("LOG_COMPRESSION_ENABLED", true)
}
