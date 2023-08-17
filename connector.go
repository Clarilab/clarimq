package gorabbitmq

import (
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"strconv"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	publish string = "publish"
	consume string = "consume"

	closeWGDelta int = 2
)

// Connector manages the connection to a RabbitMQ cluster.
type Connector struct {
	options *ConnectorOptions

	publishConn    *amqp.Connection
	publishChannel *amqp.Channel
	publishCloseWG *sync.WaitGroup

	consumeConn    *amqp.Connection
	consumeChannel *amqp.Channel
	consumeCloseWG *sync.WaitGroup
}

// NewConnector creates a new Connector instance.
//
// Needs to be closed with the Close() method.
func NewConnector(settings *ConnectionSettings, options ...ConnectorOption) *Connector {
	opt := defaultConnectorOptions(
		fmt.Sprintf("amqp://%s:%s@%s/",
			url.QueryEscape(settings.UserName),
			url.QueryEscape(settings.Password),
			net.JoinHostPort(
				url.QueryEscape(settings.Host),
				strconv.Itoa(settings.Port),
			),
		),
	)

	for i := 0; i < len(options); i++ {
		options[i](opt)
	}

	logger := slog.New(opt.LogHandler)

	slog.SetDefault(logger)

	return &Connector{
		options:        opt,
		publishCloseWG: &sync.WaitGroup{},
		consumeCloseWG: &sync.WaitGroup{},
	}
}

// Close gracefully closes the connection to the server.
func (c *Connector) Close() error {
	const errMessage = "failed to close connections to rabbitmq gracefully: %w"

	var err error

	if c.publishConn != nil {
		slog.Debug("closing connection", "type", publishType)

		c.publishCloseWG.Add(closeWGDelta)

		if err = c.publishConn.Close(); err != nil {
			return fmt.Errorf(errMessage, err)
		}

		c.publishCloseWG.Wait()
	}

	if c.consumeConn != nil {
		slog.Debug("closing connection", "type", consumeType)

		c.consumeCloseWG.Add(closeWGDelta)

		if err = c.consumeConn.Close(); err != nil {
			return fmt.Errorf(errMessage, err)
		}

		c.consumeCloseWG.Wait()
	}

	slog.Info("gracefully closed connections to rabbitmq")

	return nil
}

// DecodeDeliveryBody can be used to decode the body of a delivery into v.
func (c *Connector) DecodeDeliveryBody(delivery Delivery, v any) error {
	const errMessage = "failed to decode delivery body: %w"

	if err := c.options.Codec.Decoder(delivery.Body, v); err != nil {
		return fmt.Errorf(errMessage, err)
	}

	return nil
}

type connectParams struct {
	conn         *amqp.Connection
	channel      *amqp.Channel
	opt          *ConnectorOptions
	closeWG      *sync.WaitGroup
	instanceType string
}

func connect(params *connectParams) (*amqp.Connection, *amqp.Channel, error) {
	const errMessage = "failed to connect to rabbitmq: %w"

	conn := params.conn
	channel := params.channel
	var err error

	if params.conn == nil {
		conn, err = amqp.DialConfig(params.opt.uri, amqp.Config(*params.opt.Config))
		if err != nil {
			return nil, nil, fmt.Errorf(errMessage, err)
		}

		go watchConnectionNotifications(conn, publish, params.closeWG)

		channel, err = createChannel(conn, params.opt.PrefetchCount)
		if err != nil {
			return nil, nil, fmt.Errorf(errMessage, err)
		}

		go watchChannelNotifications(channel, publish, params.opt.ReturnHandler, params.closeWG)
	}

	return conn, channel, nil
}

func createChannel(conn *amqp.Connection, prefetchCount int) (*amqp.Channel, error) {
	const errMessage = "failed to create channel: %w"

	channel, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf(errMessage, err)
	}

	if prefetchCount > 0 {
		err = channel.Qos(prefetchCount, 0, false)
		if err != nil {
			return nil, fmt.Errorf(errMessage, err)
		}
	}

	return channel, nil
}

func watchConnectionNotifications(conn *amqp.Connection, name string, closeWG *sync.WaitGroup) {
	for {
		select {
		case err := <-conn.NotifyClose(make(chan *amqp.Error)):
			if err == nil {
				slog.Debug("closed connection", "type", name)

				closeWG.Done()

				return
			}

			//nolint: godox // follow-up task
			// TODO: reconnect logic
			slog.Debug("connection unexpectedly closed", "type", name, "cause", err)

		case block := <-conn.NotifyBlocked(make(chan amqp.Blocking)):
			slog.Warn("connection exception", "type", name, "cause", block.Reason)
		}
	}
}

func watchChannelNotifications(channel *amqp.Channel, name string, returnHandler ReturnHandler, closeWG *sync.WaitGroup) {
	for {
		select {
		case err := <-channel.NotifyClose(make(chan *amqp.Error)):
			if err == nil {
				slog.Debug("closed channel", "type", name)

				closeWG.Done()

				return
			}

			//nolint: godox // follow-up task
			// TODO: reconnect logic
			slog.Debug("channel unexpectedly closed", "type", name, "cause", err)

		case tag := <-channel.NotifyCancel(make(chan string)):
			slog.Warn("cancel exception", "type", name, "cause", tag)

		case rtrn := <-channel.NotifyReturn(make(chan amqp.Return)):
			if returnHandler != nil {
				returnHandler(Return(rtrn))

				continue
			}

			slog.Warn(
				"message could not be published",
				"replyCode", rtrn.ReplyCode,
				"replyText", rtrn.ReplyText,
				"messageId", rtrn.MessageId,
				"correlationId", rtrn.CorrelationId,
				"exchange", rtrn.Exchange,
				"routingKey", rtrn.RoutingKey,
			)
		}
	}
}
