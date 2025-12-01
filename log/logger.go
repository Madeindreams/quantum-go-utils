package log

import (
	"os"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const logKeyError = "error"

var (
	logger   *zap.SugaredLogger
	logLevel zapcore.Level
)

type Level int32

const (
	DebugLevel Level = iota
	InfoLevel
	WarnLevel
	ErrorLevel
	FatalLevel
)

func init() {
	zapConfig := zap.NewDevelopmentConfig()

	zapConfig.OutputPaths = []string{"stdout"}
	zapConfig.ErrorOutputPaths = []string{"stdout"}

	// Force console-style log output instead of JSON
	zapConfig.Encoding = "console"

	// Define log format to include time, level, and message
	zapConfig.EncoderConfig = zapcore.EncoderConfig{
		TimeKey:      "time",
		LevelKey:     "level",
		MessageKey:   "msg",
		CallerKey:    "caller",
		EncodeLevel:  zapcore.CapitalColorLevelEncoder,
		EncodeTime:   zapcore.ISO8601TimeEncoder,
		EncodeCaller: zapcore.ShortCallerEncoder,
	}

	configuredLogLevel, ok := os.LookupEnv("LOG_LEVEL")
	if ok {
		switch strings.ToLower(configuredLogLevel) {
		case "debug":
			logLevel = zapcore.DebugLevel
		case "info":
			logLevel = zapcore.InfoLevel
		case "warn":
			logLevel = zapcore.WarnLevel
		case "error":
			logLevel = zapcore.ErrorLevel
		case "fatal":
			logLevel = zapcore.FatalLevel
		}
	} else {
		logLevel = zapcore.InfoLevel
	}
	zapConfig.Level = zap.NewAtomicLevelAt(logLevel)
	zapLogger, _ := zapConfig.Build(zap.AddCallerSkip(1))
	logger = zapLogger.Sugar()
}

func IsDebugMode() bool {
	return logLevel == zapcore.DebugLevel
}

func Fatal(structuredLogMessage string, logKeysWithValues ...interface{}) {
	logger.Fatalw(structuredLogMessage, logKeysWithValues...)
}

func FatalErr(structuredLogMessage string, err error, logKeysWithValues ...interface{}) {
	logKeysWithValues = append(logKeysWithValues, logKeyError, err)
	logger.Fatalw(structuredLogMessage, logKeysWithValues...)
}

func Error(structuredLogMessage string, logKeysWithValues ...interface{}) {
	logger.Errorw(structuredLogMessage, logKeysWithValues...)
}

func ErrorErr(structuredLogMessage string, err error, logKeysWithValues ...interface{}) {
	logKeysWithValues = append(logKeysWithValues, logKeyError, err)
	logger.Errorw(structuredLogMessage, logKeysWithValues...)
}

func Warn(structuredLogMessage string, logKeysWithValues ...interface{}) {
	logger.Warnw(structuredLogMessage, logKeysWithValues...)
}

func WarnErr(structuredLogMessage string, err error, logKeysWithValues ...interface{}) {
	logKeysWithValues = append(logKeysWithValues, logKeyError, err)
	logger.Warnw(structuredLogMessage, logKeysWithValues...)
}

func Debug(structuredLogMessage string, logKeysWithValues ...interface{}) {
	logger.Debugw(structuredLogMessage, logKeysWithValues...)
}

func DebugErr(structuredLogMessage string, err error, logKeysWithValues ...interface{}) {
	logKeysWithValues = append(logKeysWithValues, logKeyError, err)
	logger.Debugw(structuredLogMessage, logKeysWithValues...)
}

func Info(structuredLogMessage string, logKeysWithValues ...interface{}) {
	logger.Infow(structuredLogMessage, logKeysWithValues...)
}

func InfoErr(structuredLogMessage string, err error, logKeysWithValues ...interface{}) {
	logKeysWithValues = append(logKeysWithValues, logKeyError, err)
	logger.Infow(structuredLogMessage, logKeysWithValues...)
}

func LogAtLevel(lvl Level, structuredLogMessage string, logKeysWithValues ...interface{}) {
	switch lvl {
	case DebugLevel:
		logger.Debugw(structuredLogMessage, includeTraceID(logKeysWithValues...)...)
	default:
		fallthrough
	case InfoLevel:
		logger.Infow(structuredLogMessage, includeTraceID(logKeysWithValues...)...)
	case WarnLevel:
		logger.Warnw(structuredLogMessage, includeTraceID(logKeysWithValues...)...)
	case ErrorLevel:
		logger.Errorw(structuredLogMessage, includeTraceID(logKeysWithValues...)...)
	case FatalLevel:
		logger.Fatalw(structuredLogMessage, includeTraceID(logKeysWithValues...)...)
	}
}

func includeTraceID(logKeysWithValues ...interface{}) []interface{} {
	traceID := "traceID"

	logKeysWithValues = append(logKeysWithValues, traceID)

	return logKeysWithValues
}
