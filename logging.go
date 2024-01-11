package clarimq

// Logger is an interface that is be used for log messages.
type Logger interface {
	Debug(msg string, args ...any)
	Error(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
}

type logger struct {
	loggers []Logger
}

func newLogger(loggers []Logger) *logger {
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
