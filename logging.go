package clarimq

import (
	"context"
)

// Logger is an interface that is be used for log messages.
type Logger interface {
	Debug(ctx context.Context, msg string, args ...any)
	Error(ctx context.Context, msg string, err error, args ...any)
	Info(ctx context.Context, msg string, args ...any)
	Warn(ctx context.Context, msg string, args ...any)
}

type logger struct {
	loggers []Logger
}

func newLogger(loggers []Logger) *logger {
	return &logger{loggers}
}

func (l *logger) logDebug(ctx context.Context, msg string, args ...any) {
	for i := range l.loggers {
		l.loggers[i].Debug(ctx, msg, args...)
	}
}

func (l *logger) logError(ctx context.Context, msg string, err error, args ...any) { //nolint:unparam // thats okay
	for i := range l.loggers {
		l.loggers[i].Error(ctx, msg, err, args...)
	}
}

func (l *logger) logInfo(ctx context.Context, msg string, args ...any) {
	for i := range l.loggers {
		l.loggers[i].Info(ctx, msg, args...)
	}
}

func (l *logger) logWarn(ctx context.Context, msg string, args ...any) {
	for i := range l.loggers {
		l.loggers[i].Warn(ctx, msg, args...)
	}
}
