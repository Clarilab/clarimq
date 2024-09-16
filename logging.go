package clarimq

import (
	"context"
	"log/slog"
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

// SlogLogger is a clarimq.Logger implementation that uses slog.Logger.
type SlogLogger struct {
	logger *slog.Logger
}

// Debug logs a debug message with the provided attributes.
func (s *SlogLogger) Debug(ctx context.Context, msg string, attrs ...any) {
	s.logger.DebugContext(ctx, msg, attrs...)
}

// Info logs an info message with the provided attributes.
func (s *SlogLogger) Info(ctx context.Context, msg string, attrs ...any) {
	s.logger.InfoContext(ctx, msg, attrs...)
}

// Warn logs a warning message with the provided attributes.
func (s *SlogLogger) Warn(ctx context.Context, msg string, attrs ...any) {
	s.logger.WarnContext(ctx, msg, attrs...)
}

// Error logs an error message with the provided attributes and error.
func (s *SlogLogger) Error(ctx context.Context, msg string, err error, attrs ...any) {
	if err != nil {
		attrs = append(attrs, "error", err.Error())
	}

	s.logger.ErrorContext(ctx, msg, attrs...)
}

// NewSlogLogger creates a new instance of SlogLogger.
// If a logger is not provided, it will use the default slog.Logger.
//
// Parameters:
// - logger: A pointer to a slog.Logger instance. If nil, it will use the default logger.
//
// Returns:
// - A new SlogLogger instance that implements the clarimq.Logger.
func NewSlogLogger(logger *slog.Logger) *SlogLogger {
	if logger == nil {
		logger = slog.Default()
	}

	return &SlogLogger{logger}
}
