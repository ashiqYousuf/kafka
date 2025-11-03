package server

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/ashiqYousuf/kafka/internal/config"
	"github.com/ashiqYousuf/kafka/internal/handler"
	"github.com/ashiqYousuf/kafka/internal/kafka/admin"
	"github.com/ashiqYousuf/kafka/internal/kafka/consumer"
	"github.com/ashiqYousuf/kafka/internal/kafka/producer"
	"github.com/ashiqYousuf/kafka/pkg/constants"
	"github.com/ashiqYousuf/kafka/pkg/logger"
	"github.com/ashiqYousuf/kafka/pkg/utils"
	"go.uber.org/zap"
)

func Start() {
	rootCtx, cancel := context.WithCancel(logger.WithRqId(context.Background(), utils.GenReqId()))
	defer cancel()

	logger.LoggerInit()

	if err := admin.InitKafkaAdminClient(rootCtx); err != nil {
		logger.Logger(rootCtx).Fatal("kafka init error", zap.Error(err))
	}

	if !config.GetConfig().KafkaConfig.DisableTopicCreation {
		kafkaAdminOps(rootCtx)
	}

	if err := producer.InitProducerClient(rootCtx); err != nil {
		logger.Logger(rootCtx).Fatal("kafka producer init error", zap.Error(err))
	}

	if err := consumer.InitConsumer(
		rootCtx,
		config.GetConfig().KafkaConfig.KafkaConsumerConfig.GroupID,
		config.GetConfig().KafkaConfig.KafkaConsumerConfig.Topics,
	); err != nil {
		logger.Logger(rootCtx).Fatal("kafka consumer init error", zap.Error(err))
	}

	go consumer.GetConsumerGroupClient().Start(rootCtx, func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		// processing logic
		logger.Logger(ctx).Info(
			"received message",
			zap.String("timestamp", msg.Timestamp.Local().String()),
			zap.String("topic", msg.Topic),
			zap.Int32("partition", msg.Partition),
			zap.String("key", string(msg.Key)),
			zap.Int64("offset", msg.Offset),
			zap.String("value", string(msg.Value)),
		)
		// simulate work
		time.Sleep(time.Millisecond * 100)
		return nil
	})

	mux := http.NewServeMux()
	setUpRoutes(mux)

	addr := fmt.Sprintf("127.0.0.1:%d", config.GetConfig().ServiceConfig.HTTPPort)
	server := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		logger.Logger(rootCtx).Info("server listening", zap.String(constants.PORT, addr))
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Logger(rootCtx).Fatal("error closing server", zap.Error(err))
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	cancel() // cancel the downstreams like consumer process

	logger.Logger(rootCtx).Info("graceful server shutdown started")
	shutdownCtx, cancelTimeout := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelTimeout()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Logger(shutdownCtx).Fatal("server forced to shutdown", zap.Error(err))
	}

	logger.Logger(shutdownCtx).Info("server exited cleanly")
}

func setUpRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /users", handler.CreateUserHandler)
}
