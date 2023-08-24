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
	ConnectorOption func(*ConnectorOptions)

	// Config is used in DialConfig and Open to specify the desired tuning
	// parameters used during a connection open handshake. The negotiated tuning
	// will be stored in the returned connection's Config field.
	Config amqp.Config

	// ConnectorOptions are used to describe how a new connector will be created.
	ConnectorOptions struct {
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

	// ConnectionSettings holds settings for a rabbitMQConnector connection.
	ConnectionSettings struct {
		// UserName contains the username of the rabbitMQ user.
		UserName string
		// Password contains the password of the rabbitMQ user.
		Password string
		// Host contains the hostname or ip of the rabbitMQ server.
		Host string
		// Post contains the port number the rabbitMQ server is listening on.
		Port int
	}

	ReturnHandler func(Return)
)

func defaultConnectorOptions(uri string) *ConnectorOptions {
	return &ConnectorOptions{
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

// WithCustomConnectorOptions sets the connector options.
//
// It can be used to set all connector options at once.
func WithCustomConnectorOptions(options *ConnectorOptions) ConnectorOption {
	return func(opt *ConnectorOptions) {
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

// WithConnectorOptionConnectionName sets the name of the connection.
func WithConnectorOptionConnectionName(name string) ConnectorOption {
	return func(options *ConnectorOptions) { options.Config.Properties.SetClientConnectionName(name) }
}

// WithConnectorOptionTextLogging enables structured text logging to the given writer.
func WithConnectorOptionTextLogging(w io.Writer, logLevel slog.Level) ConnectorOption {
	return func(o *ConnectorOptions) {
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

// WithConnectorOptionJSONLogging enables structured json logging to the given writer.
func WithConnectorOptionJSONLogging(w io.Writer, logLevel slog.Level) ConnectorOption {
	return func(o *ConnectorOptions) {
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

// WithConnectorOptionAMQPConfig sets the amqp.Config that will be used to create the connection.
//
// Warning: this will override any values set in the connection config.
func WithConnectorOptionAMQPConfig(config *Config) ConnectorOption {
	return func(o *ConnectorOptions) { o.Config = config }
}

// WithConnectorOptionPrefetchCount sets the number of messages that will be prefetched.
func WithConnectorOptionPrefetchCount(count int) ConnectorOption {
	return func(o *ConnectorOptions) { o.PrefetchCount = count }
}

// WithConnectorOptionEncoder sets the encoder that will be used to encode messages.
func WithConnectorOptionEncoder(encoder JSONEncoder) ConnectorOption {
	return func(options *ConnectorOptions) { options.Codec.Encoder = encoder }
}

// WithConnectorOptionDecoder sets the decoder that will be used to decode messages.
func WithConnectorOptionDecoder(decoder JSONDecoder) ConnectorOption {
	return func(options *ConnectorOptions) { options.Codec.Decoder = decoder }
}

// WithConnectorOptionReturnHandler sets an Handler that can be used to handle undeliverable publishes.
//
// When a publish is undeliverable from being mandatory, it will be returned and can be handled
// by this return handler.
func WithConnectorOptionReturnHandler(returnHandler ReturnHandler) ConnectorOption {
	return func(options *ConnectorOptions) { options.ReturnHandler = returnHandler }
}

// WithConnectorOptionReconnectInterval sets the initial reconnection interval.
//
// Default: 1s.
func WithConnectorOptionReconnectInterval(interval time.Duration) ConnectorOption {
	return func(options *ConnectorOptions) { options.ReconnectInterval = interval }
}

// WithConnectorOptionMaxReconnectRetries sets the limit for maximum retries.
//
// Default: 10.
func WithConnectorOptionMaxReconnectRetries(maxRetries int) ConnectorOption {
	return func(options *ConnectorOptions) { options.MaxReconnectRetries = maxRetries }
}

// WithConnectorOptionBackoffFactor sets the exponential backoff factor.
//
// Default: 2.
func WithConnectorOptionBackoffFactor(factor int) ConnectorOption {
	return func(options *ConnectorOptions) { options.BackoffFactor = factor }
}
