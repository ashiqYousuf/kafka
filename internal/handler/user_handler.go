package handler

import (
	"context"
	"encoding/json"
	"io"
	"net/http"

	"github.com/ashiqYousuf/kafka/internal/service"
	"github.com/ashiqYousuf/kafka/pkg/logger"
	"github.com/ashiqYousuf/kafka/pkg/utils"
	"go.uber.org/zap"
)

func CreateUserHandler(w http.ResponseWriter, r *http.Request) {
	ctx := logger.WithRqId(context.Background(), utils.GenReqId())
	logger.Logger(ctx).Info("create user handler")

	body, err := io.ReadAll(r.Body)
	if err != nil {
		logger.Logger(ctx).Error("error reading request body", zap.Error(err))
		errorResponse(ctx, err, http.StatusInternalServerError, w)
		return
	}
	defer r.Body.Close()

	resp, err := service.CreateUser(ctx, body)
	if err != nil {
		logger.Logger(ctx).Error("error reading request body", zap.Error(err))
		errorResponse(ctx, err, http.StatusInternalServerError, w)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(resp)
}

func errorResponse(ctx context.Context, err error, status int, w http.ResponseWriter) {
	resp := map[string]interface{}{
		"error": err.Error(),
	}
	d, err := json.Marshal(resp)
	if err != nil {
		logger.Logger(ctx).Error("error marshalling request", zap.Error(err))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(status)
	w.Write(d)
}
