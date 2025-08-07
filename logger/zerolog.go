package logger

import (
	"os"

	"github.com/rs/zerolog"
	"gopkg.in/natefinch/lumberjack.v2"
)

type Config struct {
	Debug      bool   `yaml:"debug"`
	FileName   string `yaml:"file-name"`
	MaxSize    int    `yaml:"max-size"`
	MaxBackups int  `yaml:"max-backups"`
	MaxAge     int  `yaml:"max-age"`
	Compress   bool `yaml:"compress"`
}

func NewLogger(cfg *Config) Logger {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	if cfg.Debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}

	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

	writer := zerolog.MultiLevelWriter(os.Stdout)
	if cfg.FileName != "" {
		lj := &lumberjack.Logger{
			Filename:   cfg.FileName,
			MaxSize:    cfg.MaxSize,
			MaxBackups: cfg.MaxBackups,
			MaxAge:     cfg.MaxAge,
			Compress:   cfg.Compress,
		}
		writer = zerolog.MultiLevelWriter(lj)
	}

	return newLogger(zerolog.New(writer).With().Timestamp().Logger())
}

type zerologLogger struct {
	zerolog.Logger
}

func newLogger(l zerolog.Logger) Logger {
	return &zerologLogger{
		Logger: l,
	}
}

func (l *zerologLogger) Info(msg string, fields ...Field) {
	l.Logger.Info().Fields(fieldsToMap(fields)).Msg(msg)
}

func (l *zerologLogger) Infof(format string, args ...any) {
	l.Logger.Info().Msgf(format, args...)
}

func (l *zerologLogger) Debug(msg string, fields ...Field) {
	l.Logger.Debug().Fields(fieldsToMap(fields)).Msg(msg)
}

func (l *zerologLogger) Debugf(format string, args ...any) {
	l.Logger.Debug().Msgf(format, args...)
}

func (l *zerologLogger) Warn(msg string, fields ...Field) {
	l.Logger.Warn().Fields(fieldsToMap(fields)).Msg(msg)
}

func (l *zerologLogger) Warnf(format string, args ...any) {
	l.Logger.Warn().Msgf(format, args...)
}

func (l *zerologLogger) Error(msg string, fields ...Field) {
	l.Logger.Error().Fields(fieldsToMap(fields)).Msg(msg)
}

func (l *zerologLogger) Errorf(format string, args ...any) {
	l.Logger.Error().Msgf(format, args...)
}

func (l *zerologLogger) Fatal(msg string, fields ...Field) {
	l.Logger.Fatal().Fields(fieldsToMap(fields)).Msg(msg)
}

func (l *zerologLogger) Fatalf(format string, args ...any) {
	l.Logger.Fatal().Msgf(format, args...)
}

func (l *zerologLogger) Panic(msg string, fields ...Field) {
	l.Logger.Panic().Fields(fieldsToMap(fields)).Msg(msg)
}

func (l *zerologLogger) Panicf(format string, args ...any) {
	l.Logger.Panic().Msgf(format, args...)
}

func (l *zerologLogger) WithFields(fields ...Field) Logger {
	return &zerologLogger{
		Logger: l.Logger.With().Fields(fieldsToMap(fields)).Logger(),
	}
}

func (l *zerologLogger) WithField(key string, value any) Logger {
	return &zerologLogger{
		Logger: l.Logger.With().Interface(key, value).Logger(),
	}
}

func fieldsToMap(fields []Field) map[string]interface{} {
	m := make(map[string]interface{}, len(fields))
	for _, f := range fields {
		m[f.Key] = f.Value
	}
	return m
}
