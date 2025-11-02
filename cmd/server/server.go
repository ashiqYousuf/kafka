package server

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ashiqYousuf/kafka/internal/config"
	"github.com/ashiqYousuf/kafka/internal/handler"
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

	if err := producer.InitProducerClient(ctx); err != nil {
		logger.Logger(ctx).Fatal("kafka producer init error", zap.Error(err))
	}

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
		logger.Logger(ctx).Info("server listening", zap.String(constants.PORT, addr))
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Logger(ctx).Fatal("error closing server", zap.Error(err))
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Logger(ctx).Info("graceful server shutdown started")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		logger.Logger(ctx).Fatal("server forced to shutdown", zap.Error(err))
	}

	logger.Logger(ctx).Info("server exited cleanly")
}

func setUpRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /users", handler.CreateUserHandler)
}
