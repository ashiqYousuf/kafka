package logger

import (
	"context"
	"log"
	"strings"

	"github.com/ashiqYousuf/kafka/internal/config"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

var Log *zap.Logger

type customType int

const (
	requestIdKey customType = iota
	traceIdKey              = "tr"
)

type LogConfig struct {
	LogFilePath           string
	LogLevel              zapcore.Level
	LogMaxSize            int
	LogMaxAge             int
	LogMaxBackups         int
	LogCompressionEnabled bool
}

func LoggerInit() {
	level := zapcore.ErrorLevel
	err := level.Set(config.GetConfig().LogConfig.LogLevel)
	if err != nil {
		log.Fatalf("error in setting log level %v", err)
	}

	logConfig := &LogConfig{
		LogLevel: level,
	}
	logConfig.NewLogger()
}

func (lf *LogConfig) NewLogger() *zap.Logger {
	if strings.Compare(config.GetConfig().ServiceConfig.Env, "dev") == 0 {
		logRotate := &lumberjack.Logger{
			Filename:   config.GetConfig().LogConfig.LogFileName,
			MaxSize:    config.GetConfig().LogConfig.LogMaxSize,
			MaxAge:     config.GetConfig().LogConfig.LogMaxAge,
			MaxBackups: config.GetConfig().LogConfig.LogMaxBackups,
			Compress:   config.GetConfig().LogConfig.LogCompressionEnabled,
		}
		w := zapcore.AddSync(logRotate) // Wraps the output (stdout, file, buffer) in a thread-safe writer
		core := zapcore.NewCore(
			zapcore.NewJSONEncoder(encoderConfig()),
			w,
			lf.LogLevel,
		)
		logger := zap.New(core)
		Log = logger
		return logger
	}

	// Prod mode → logs to stdout / JSON
	var opts []zap.Option
	opts = append(opts, zap.AddStacktrace(zap.ErrorLevel), zap.AddCaller())

	prodConfig := zap.NewProductionConfig()
	prodConfig.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	prodConfig.Sampling = nil // no log dropping
	logger, _ := prodConfig.Build(opts...)

	Log = logger
	return logger
}

func encoderConfig() zapcore.EncoderConfig {
	return zapcore.EncoderConfig{
		TimeKey:        "ts",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}
}

func Logger(ctx context.Context) *zap.Logger {
	newLogger := Log
	if ctx != nil {
		if ctxRqId, ok := ctx.Value(requestIdKey).(string); ok {
			newLogger = newLogger.With(
				zap.String("rqId", ctxRqId),
				// zap.String("tr", extractTraceIdFromCtx(ctx)),
			)
		}
	}
	return newLogger
}

func WithRqId(ctx context.Context, rqId string) context.Context {
	return context.WithValue(ctx, requestIdKey, rqId)
}

func GetRqId(ctx context.Context) string {
	if ctxRqId, ok := ctx.Value(requestIdKey).(string); ok {
		return ctxRqId
	}
	return ""
}

// func extractTraceIdFromCtx(ctx context.Context) string {
// 	if ctx != nil {
// 		span := tr.SpanFromContext(ctx)
// 		traceId := span.SpanContext().TraceID().String()
// 		return traceId
// 	}
// 	return ""
// }
