package clarimq

import (
	"log/slog"
)

type logger struct {
	loggers []*slog.Logger
}

func newLogger(loggers []*slog.Logger) *logger {
	return &logger{loggers}
}

func (l *logger) logDebug(msg string, args ...any) {
	for i := range l.loggers {
		l.loggers[i].Debug(msg, args...)
	}
}

func (l *logger) logError(msg string, args ...any) {
	for i := range l.loggers {
		l.loggers[i].Error(msg, args...)
	}
}

func (l *logger) logInfo(msg string, args ...any) {
	for i := range l.loggers {
		l.loggers[i].Info(msg, args...)
	}
}

func (l *logger) logWarn(msg string, args ...any) {
	for i := range l.loggers {
		l.loggers[i].Warn(msg, args...)
	}
}
