package clarimq

import (
	"encoding/json"
	"io"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	defaultRecoveryInterval     time.Duration = time.Second
	defaultMaxRecoveryRetries   int           = 10
	defaultBackOffFactor        int           = 2
	defaultPrefetchCount        int           = 0
	defaultConnectionNamePrefix string        = "connection_"
	amqpConnectionNameKey       string        = "connection_name"
)

type (
	// ConnectionOption is an option for a Connection.
	ConnectionOption func(*ConnectionOptions)

	// Config is used in DialConfig and Open to specify the desired tuning
	// parameters used during a connection open handshake. The negotiated tuning
	// will be stored in the returned connection's Config field.
	Config amqp.Config

	// ConnectionOptions are used to describe how a new connection will be created.
	ConnectionOptions struct {
		ReturnHandler
		loggers            []*slog.Logger
		Config             *Config
		codec              *codec
		uri                string
		PrefetchCount      int
		RecoveryInterval   time.Duration
		MaxRecoveryRetries int
		BackOffFactor      int
	}

	// ConnectionSettings holds settings for a broker connection.
	ConnectionSettings struct {
		// UserName contains the username of the broker user.
		UserName string
		// Password contains the password of the broker user.
		Password string
		// Host contains the hostname or ip of the broker.
		Host string
		// Post contains the port number the broker is listening on.
		Port int
	}

	ReturnHandler func(Return)
)

func defaultConnectionOptions(uri string) *ConnectionOptions {
	return &ConnectionOptions{
		uri:                uri,
		RecoveryInterval:   defaultRecoveryInterval,
		MaxRecoveryRetries: defaultMaxRecoveryRetries,
		BackOffFactor:      defaultBackOffFactor,
		Config: &Config{
			Properties: amqp.Table{
				amqpConnectionNameKey: defaultConnectionNamePrefix + newRandomString(),
			},
		},
		PrefetchCount: defaultPrefetchCount,
		codec: &codec{
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
			opt.RecoveryInterval = options.RecoveryInterval

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
		o.loggers = append(o.loggers,
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
		o.loggers = append(o.loggers,
			slog.New(slog.NewJSONHandler(
				w,
				&slog.HandlerOptions{
					Level: logLevel,
				},
			)),
		)
	}
}

// WithConnectionOptionMultipleLoggers adds multiple loggers.
func WithConnectionOptionMultipleLoggers(loggers []*slog.Logger) ConnectionOption {
	return func(o *ConnectionOptions) {
		o.loggers = append(o.loggers, loggers...)
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
	return func(options *ConnectionOptions) { options.codec.Encoder = encoder }
}

// WithConnectionOptionDecoder sets the decoder that will be used to decode messages.
func WithConnectionOptionDecoder(decoder JSONDecoder) ConnectionOption {
	return func(options *ConnectionOptions) { options.codec.Decoder = decoder }
}

// WithConnectionOptionReturnHandler sets an Handler that can be used to handle undeliverable publishes.
//
// When a publish is undeliverable from being mandatory, it will be returned and can be handled
// by this return handler.
func WithConnectionOptionReturnHandler(returnHandler ReturnHandler) ConnectionOption {
	return func(options *ConnectionOptions) { options.ReturnHandler = returnHandler }
}

// WithConnectionOptionRecoveryInterval sets the initial recovery interval.
//
// Default: 1s.
func WithConnectionOptionRecoveryInterval(interval time.Duration) ConnectionOption {
	return func(options *ConnectionOptions) { options.RecoveryInterval = interval }
}

// WithConnectionOptionMaxRecoveryRetries sets the limit for maximum retries.
//
// Default: 10.
func WithConnectionOptionMaxRecoveryRetries(maxRetries int) ConnectionOption {
	return func(options *ConnectionOptions) { options.MaxRecoveryRetries = maxRetries }
}

// WithConnectionOptionBackOffFactor sets the exponential back-off factor.
//
// Default: 2.
func WithConnectionOptionBackOffFactor(factor int) ConnectionOption {
	return func(options *ConnectionOptions) { options.BackOffFactor = factor }
}

// SetLoggers provides possibility to add loggers.
func (c *Connection) SetLoggers(loggers []*slog.Logger) {
	if len(loggers) > 0 {
		c.options.loggers = loggers
	}
}

// SetReturnHandler provides possibility to set the json encoder.
func (c *Connection) SetEncoder(encoder JSONEncoder) {
	if encoder != nil {
		c.options.codec.Encoder = encoder
	}
}

// SetReturnHandler provides possibility to set the json decoder.
func (c *Connection) SetDecoder(decoder JSONDecoder) {
	if decoder != nil {
		c.options.codec.Decoder = decoder
	}
}

// SetReturnHandler provides possibility to add a return handler.
func (c *Connection) SetReturnHandler(returnHandler ReturnHandler) {
	if returnHandler != nil {
		c.returnHandler = returnHandler
	}
}

// SetRecoveryInterval sets the recovery interval.
//
// Default: 1s.
func (c *Connection) SetRecoveryInterval(interval time.Duration) {
	c.options.RecoveryInterval = interval
}

// SetMaxRecoveryRetries sets the limit for maximum retries.
//
// Default: 10.
func (c *Connection) SetMaxRecoveryRetries(maxRetries int) {
	c.options.MaxRecoveryRetries = maxRetries
}

// SetBackOffFactor sets the exponential back-off factor.
//
// Default: 2.
func (c *Connection) SetBackOffFactor(factor int) {
	c.options.BackOffFactor = factor
}
