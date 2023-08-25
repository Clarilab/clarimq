package gorabbitmq

import (
	"encoding/json"
	"io"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	defaultReconnectInterval   time.Duration = time.Second
	defaultMaxReconnectRetries int           = 10
	defaultBackoffFactor       int           = 2
	defaultPrefetchCount       int           = 0
)

type (
	ConnectionOption func(*ConnectionOptions)

	// Config is used in DialConfig and Open to specify the desired tuning
	// parameters used during a connection open handshake. The negotiated tuning
	// will be stored in the returned connection's Config field.
	Config amqp.Config

	// ConnectionOptions are used to describe how a new connection will be created.
	ConnectionOptions struct {
		ReturnHandler
		logger              []*slog.Logger
		Config              *Config
		Codec               *codec
		uri                 string
		PrefetchCount       int
		ReconnectInterval   time.Duration
		MaxReconnectRetries int
		BackoffFactor       int
	}

	// ConnectionSettings holds settings for a RabbitMQ connection.
	ConnectionSettings struct {
		// UserName contains the username of the RabbitMQ user.
		UserName string
		// Password contains the password of the RabbitMQ user.
		Password string
		// Host contains the hostname or ip of the RabbitMQ server.
		Host string
		// Post contains the port number the RabbitMQ server is listening on.
		Port int
	}

	ReturnHandler func(Return)
)

func defaultConnectionOptions(uri string) *ConnectionOptions {
	return &ConnectionOptions{
		uri:                 uri,
		ReconnectInterval:   defaultReconnectInterval,
		MaxReconnectRetries: defaultMaxReconnectRetries,
		BackoffFactor:       defaultBackoffFactor,
		Config: &Config{
			Properties: make(amqp.Table),
		},
		PrefetchCount: defaultPrefetchCount,
		Codec: &codec{
			Encoder: json.Marshal,
			Decoder: json.Unmarshal,
		},
	}
}

// WithCustomConnectionOptions sets the connection options.
//
// It can be used to set all connection options at once.
func WithCustomConnectionOptions(options *ConnectionOptions) ConnectionOption {
	return func(opt *ConnectionOptions) {
		if options != nil {
			opt.PrefetchCount = options.PrefetchCount
			opt.ReconnectInterval = options.ReconnectInterval

			if options.Config != nil {
				opt.Config = options.Config
			}

			if options.ReturnHandler != nil {
				opt.ReturnHandler = options.ReturnHandler
			}
		}
	}
}

// WithConnectionOptionConnectionName sets the name of the connection.
func WithConnectionOptionConnectionName(name string) ConnectionOption {
	return func(options *ConnectionOptions) { options.Config.Properties.SetClientConnectionName(name) }
}

// WithConnectionOptionTextLogging enables structured text logging to the given writer.
func WithConnectionOptionTextLogging(w io.Writer, logLevel slog.Level) ConnectionOption {
	return func(o *ConnectionOptions) {
		o.logger = append(o.logger,
			slog.New(slog.NewTextHandler(
				w,
				&slog.HandlerOptions{
					Level: logLevel,
				},
			)),
		)
	}
}

// WithConnectionOptionJSONLogging enables structured json logging to the given writer.
func WithConnectionOptionJSONLogging(w io.Writer, logLevel slog.Level) ConnectionOption {
	return func(o *ConnectionOptions) {
		o.logger = append(o.logger,
			slog.New(slog.NewJSONHandler(
				w,
				&slog.HandlerOptions{
					Level: logLevel,
				},
			)),
		)
	}
}

// WithConnectionOptionAMQPConfig sets the amqp.Config that will be used to create the connection.
//
// Warning: this will override any values set in the connection config.
func WithConnectionOptionAMQPConfig(config *Config) ConnectionOption {
	return func(o *ConnectionOptions) { o.Config = config }
}

// WithConnectionOptionPrefetchCount sets the number of messages that will be prefetched.
func WithConnectionOptionPrefetchCount(count int) ConnectionOption {
	return func(o *ConnectionOptions) { o.PrefetchCount = count }
}

// WithConnectionOptionEncoder sets the encoder that will be used to encode messages.
func WithConnectionOptionEncoder(encoder JSONEncoder) ConnectionOption {
	return func(options *ConnectionOptions) { options.Codec.Encoder = encoder }
}

// WithConnectionOptionDecoder sets the decoder that will be used to decode messages.
func WithConnectionOptionDecoder(decoder JSONDecoder) ConnectionOption {
	return func(options *ConnectionOptions) { options.Codec.Decoder = decoder }
}

// WithConnectionOptionReturnHandler sets an Handler that can be used to handle undeliverable publishes.
//
// When a publish is undeliverable from being mandatory, it will be returned and can be handled
// by this return handler.
func WithConnectionOptionReturnHandler(returnHandler ReturnHandler) ConnectionOption {
	return func(options *ConnectionOptions) { options.ReturnHandler = returnHandler }
}

// WithConnectionOptionReconnectInterval sets the initial reconnection interval.
//
// Default: 1s.
func WithConnectionOptionReconnectInterval(interval time.Duration) ConnectionOption {
	return func(options *ConnectionOptions) { options.ReconnectInterval = interval }
}

// WithConnectionOptionMaxReconnectRetries sets the limit for maximum retries.
//
// Default: 10.
func WithConnectionOptionMaxReconnectRetries(maxRetries int) ConnectionOption {
	return func(options *ConnectionOptions) { options.MaxReconnectRetries = maxRetries }
}

// WithConnectionOptionBackoffFactor sets the exponential backoff factor.
//
// Default: 2.
func WithConnectionOptionBackoffFactor(factor int) ConnectionOption {
	return func(options *ConnectionOptions) { options.BackoffFactor = factor }
}
