package logger

type Logger interface {
	Info(msg string, fields ...Field)
	Infof(format string, args ...any)

	Debug(msg string, fields ...Field)
	Debugf(format string, args ...any)

	Warn(msg string, fields ...Field)
	Warnf(format string, args ...any)

	Error(msg string, fields ...Field)
	Errorf(format string, args ...any)

	Fatal(msg string, fields ...Field)
	Fatalf(format string, args ...any)

	Panic(msg string, fields ...Field)
	Panicf(format string, args ...any)

	WithFields(fields ...Field) Logger
	WithField(key string, value any) Logger
}

type Field struct {
	Key   string
	Value any
}
