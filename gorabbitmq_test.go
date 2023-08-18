package gorabbitmq_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/Clarilab/gorabbitmq/v5"
	amqp "github.com/rabbitmq/amqp091-go"
)

type testData struct {
	Name    string `json:"name"`
	Age     int    `json:"age"`
	City    string `json:"city"`
	Country string `json:"country"`
}

type testParams struct {
	exchangeName string
	queueName    string
	routingKey   string
}

func Test_Integration_PublishToExchange(t *testing.T) {
	t.Parallel()

	stringMessage := "test-message"
	bytesMessage := []byte(stringMessage)

	jsonMessage := testData{
		Name:    "Name",
		Age:     157,
		City:    "City",
		Country: "Country",
	}

	tests := map[string]struct {
		deliveryHandler func(any, chan struct{}) gorabbitmq.HandlerFunc
		getConsumer     func(*gorabbitmq.Connector, gorabbitmq.HandlerFunc, *testParams) (*gorabbitmq.Consumer, error)
		passiveExchange bool
		message         any
	}{
		"publish to exchange / consume with exchange NoWait": {
			deliveryHandler: func(expectedMessage any, doneChan chan struct{}) gorabbitmq.HandlerFunc {
				return func(d gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)

					doneChan <- struct{}{}

					return gorabbitmq.Ack
				}
			},
			getConsumer: func(c *gorabbitmq.Connector, handler gorabbitmq.HandlerFunc, params *testParams) (*gorabbitmq.Consumer, error) {
				return c.NewConsumerAndSubscribe(
					params.queueName,
					handler,
					gorabbitmq.WithExchangeOptionAutoDelete(true),
					gorabbitmq.WithExchangeOptionDeclare(true),
					gorabbitmq.WithExchangeOptionKind(gorabbitmq.ExchangeTopic),
					gorabbitmq.WithExchangeOptionName(params.exchangeName),
					gorabbitmq.WithExchangeOptionNoWait(true),
					gorabbitmq.WithQueueOptionAutoDelete(true),
					gorabbitmq.WithConsumerOptionRoutingKey(params.routingKey),
				)
			},

			message: stringMessage,
		},
		"publish to exchange passive": {
			deliveryHandler: func(expectedMessage any, doneChan chan struct{}) gorabbitmq.HandlerFunc {
				return func(d gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, expectedMessage, d.Body)
					requireEqual(t, "application/octet-stream", d.ContentType)

					doneChan <- struct{}{}

					return gorabbitmq.Ack
				}
			},
			getConsumer: func(c *gorabbitmq.Connector, handler gorabbitmq.HandlerFunc, params *testParams) (*gorabbitmq.Consumer, error) {
				return c.NewConsumerAndSubscribe(
					params.queueName,
					handler,
					gorabbitmq.WithExchangeOptionAutoDelete(true),
					gorabbitmq.WithExchangeOptionDeclare(true),
					gorabbitmq.WithExchangeOptionKind(gorabbitmq.ExchangeTopic),
					gorabbitmq.WithExchangeOptionName(params.exchangeName),
					gorabbitmq.WithQueueOptionAutoDelete(true),
					gorabbitmq.WithConsumerOptionRoutingKey(params.routingKey),
					gorabbitmq.WithExchangeOptionPassive(true),
				)
			},
			passiveExchange: true,
			message:         bytesMessage,
		},
		"publish bytes message": {
			deliveryHandler: func(expectedMessage any, doneChan chan struct{}) gorabbitmq.HandlerFunc {
				return func(d gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, expectedMessage, d.Body)
					requireEqual(t, "application/octet-stream", d.ContentType)

					doneChan <- struct{}{}

					return gorabbitmq.Ack
				}
			},
			getConsumer: func(c *gorabbitmq.Connector, handler gorabbitmq.HandlerFunc, params *testParams) (*gorabbitmq.Consumer, error) {
				return c.NewConsumerAndSubscribe(
					params.queueName,
					handler,
					gorabbitmq.WithExchangeOptionAutoDelete(true),
					gorabbitmq.WithExchangeOptionDeclare(true),
					gorabbitmq.WithExchangeOptionKind(gorabbitmq.ExchangeTopic),
					gorabbitmq.WithExchangeOptionName(params.exchangeName),
					gorabbitmq.WithQueueOptionAutoDelete(true),
					gorabbitmq.WithConsumerOptionRoutingKey(params.routingKey),
				)
			},

			message: bytesMessage,
		},
		"publish json message": {
			deliveryHandler: func(expectedMessage any, doneChan chan struct{}) gorabbitmq.HandlerFunc {
				return func(d gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, "application/json", d.ContentType)

					var result testData

					err := json.Unmarshal(d.Body, &result)
					requireNoError(t, err)

					requireEqual(t, expectedMessage, result)

					doneChan <- struct{}{}

					return gorabbitmq.Ack
				}
			},
			getConsumer: func(c *gorabbitmq.Connector, handler gorabbitmq.HandlerFunc, params *testParams) (*gorabbitmq.Consumer, error) {
				return c.NewConsumerAndSubscribe(
					params.queueName,
					handler,
					gorabbitmq.WithExchangeOptionAutoDelete(true),
					gorabbitmq.WithExchangeOptionDeclare(true),
					gorabbitmq.WithExchangeOptionKind(gorabbitmq.ExchangeTopic),
					gorabbitmq.WithExchangeOptionName(params.exchangeName),
					gorabbitmq.WithQueueOptionAutoDelete(true),
					gorabbitmq.WithConsumerOptionRoutingKey(params.routingKey),
				)
			},
			message: jsonMessage,
		},
	}

	for name, test := range tests {
		name, test := name, test

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			connector := getConnector()

			t.Cleanup(func() {
				err := connector.Close()
				requireNoError(t, err)
			})

			doneChan := make(chan struct{})

			testParams := &testParams{
				exchangeName: stringGen(),
				queueName:    stringGen(),
				routingKey:   stringGen(),
			}

			if test.passiveExchange {
				consumer, err := connector.NewConsumerAndSubscribe(
					testParams.queueName,
					nil,
					gorabbitmq.WithExchangeOptionDeclare(true),
					gorabbitmq.WithExchangeOptionKind(gorabbitmq.ExchangeTopic),
					gorabbitmq.WithExchangeOptionName(testParams.exchangeName),
					gorabbitmq.WithQueueOptionAutoDelete(true),
					gorabbitmq.WithConsumerOptionRoutingKey(testParams.routingKey),
				)

				requireNoError(t, err)

				err = consumer.Unsubscribe()
				requireNoError(t, err)
			}

			consumer, err := test.getConsumer(connector, test.deliveryHandler(test.message, doneChan), testParams)
			requireNoError(t, err)

			publisher, err := connector.NewPublisher(
				gorabbitmq.WithPublishOptionExchange(testParams.exchangeName),
			)
			requireNoError(t, err)

			err = publisher.Publish(context.TODO(), testParams.routingKey, test.message)
			requireNoError(t, err)

			<-doneChan

			if test.passiveExchange {
				err = consumer.RemoveExchange(testParams.exchangeName, false, false)
				requireNoError(t, err)
			}
		})
	}
}

func Test_Integration_PublishToQueue(t *testing.T) {
	t.Parallel()

	message := "test-message"

	tests := map[string]struct {
		deliveryHandler func(any, chan struct{}) gorabbitmq.HandlerFunc
		getConsumer     func(*gorabbitmq.Connector, gorabbitmq.HandlerFunc, string) (*gorabbitmq.Consumer, error)
		passiveQueue    bool
		publish         func(*gorabbitmq.Publisher, string) error
	}{
		"publish to queue": {
			deliveryHandler: func(expectedMessage any, doneChan chan struct{}) gorabbitmq.HandlerFunc {
				return func(d gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)

					doneChan <- struct{}{}

					return gorabbitmq.Ack
				}
			},
			getConsumer: func(c *gorabbitmq.Connector, handler gorabbitmq.HandlerFunc, queueName string) (*gorabbitmq.Consumer, error) {
				return c.NewConsumerAndSubscribe(
					queueName,
					handler,
					gorabbitmq.WithQueueOptionAutoDelete(true),
					gorabbitmq.WithQueueOptionArgs(gorabbitmq.Table{
						"test-queue-arg-key": "test-queue-arg-value",
					}),
				)
			},
			publish: func(p *gorabbitmq.Publisher, target string) error {
				return p.PublishWithOptions(context.TODO(), []string{target}, message)
			},
		},
		"publish to queue passive": {
			deliveryHandler: func(expectedMessage any, doneChan chan struct{}) gorabbitmq.HandlerFunc {
				return func(d gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)

					doneChan <- struct{}{}

					return gorabbitmq.Ack
				}
			},
			getConsumer: func(c *gorabbitmq.Connector, handler gorabbitmq.HandlerFunc, queueName string) (*gorabbitmq.Consumer, error) {
				return c.NewConsumerAndSubscribe(
					queueName,
					handler,
					gorabbitmq.WithQueueOptionAutoDelete(true),
					gorabbitmq.WithQueueOptionPassive(true),
				)
			},
			publish: func(p *gorabbitmq.Publisher, target string) error {
				return p.PublishWithOptions(context.TODO(), []string{target}, message)
			},
			passiveQueue: true,
		},
		"publish to queue NoWait": {
			deliveryHandler: func(expectedMessage any, doneChan chan struct{}) gorabbitmq.HandlerFunc {
				return func(d gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)

					doneChan <- struct{}{}

					return gorabbitmq.Ack
				}
			},
			getConsumer: func(c *gorabbitmq.Connector, handler gorabbitmq.HandlerFunc, queueName string) (*gorabbitmq.Consumer, error) {
				return c.NewConsumerAndSubscribe(
					queueName,
					handler,
					gorabbitmq.WithQueueOptionAutoDelete(true),
					gorabbitmq.WithQueueOptionNoWait(true),
				)
			},
			publish: func(p *gorabbitmq.Publisher, target string) error {
				return p.PublishWithOptions(context.TODO(), []string{target}, message)
			},
		},
		"publish to priority queue": {
			deliveryHandler: func(expectedMessage any, doneChan chan struct{}) gorabbitmq.HandlerFunc {
				return func(d gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)
					requireEqual(t, 4, int(d.Priority))

					doneChan <- struct{}{}

					return gorabbitmq.Ack
				}
			},
			getConsumer: func(c *gorabbitmq.Connector, handler gorabbitmq.HandlerFunc, queueName string) (*gorabbitmq.Consumer, error) {
				return c.NewConsumerAndSubscribe(
					queueName,
					handler,
					gorabbitmq.WithQueueOptionAutoDelete(true),
					gorabbitmq.WithQueueOptionPriority(gorabbitmq.HighestPriority),
				)
			},
			publish: func(p *gorabbitmq.Publisher, target string) error {
				return p.PublishWithOptions(context.TODO(), []string{target}, message, gorabbitmq.WithPublishOptionPriority(gorabbitmq.HighPriority))
			},
		},
		"publish to durable queue": {
			deliveryHandler: func(expectedMessage any, doneChan chan struct{}) gorabbitmq.HandlerFunc {
				return func(d gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)

					doneChan <- struct{}{}

					return gorabbitmq.Ack
				}
			},
			getConsumer: func(c *gorabbitmq.Connector, handler gorabbitmq.HandlerFunc, queueName string) (*gorabbitmq.Consumer, error) {
				return c.NewConsumerAndSubscribe(
					queueName,
					handler,
					gorabbitmq.WithQueueOptionAutoDelete(true),
					gorabbitmq.WithQueueOptionDurable(true),
				)
			},
			publish: func(p *gorabbitmq.Publisher, target string) error {
				return p.PublishWithOptions(context.TODO(), []string{target}, message)
			},
		},
	}

	for name, test := range tests {
		name, test := name, test

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			connector := getConnector()

			t.Cleanup(func() {
				err := connector.Close()
				requireNoError(t, err)
			})

			doneChan := make(chan struct{})
			queueName := stringGen()

			if test.passiveQueue {
				consumer, err := connector.NewConsumerAndSubscribe(queueName, nil)

				requireNoError(t, err)

				err = consumer.Unsubscribe()
				requireNoError(t, err)
			}

			consumer, err := test.getConsumer(connector, test.deliveryHandler(message, doneChan), queueName)
			requireNoError(t, err)

			publisher, err := connector.NewPublisher()
			requireNoError(t, err)

			err = test.publish(publisher, queueName)
			requireNoError(t, err)

			<-doneChan

			if test.passiveQueue {
				_, err = consumer.RemoveQueue(queueName, false, false, false)
				requireNoError(t, err)
			}
		})
	}
}

func Test_Integration_Consume(t *testing.T) {
	t.Parallel()

	message := "test-message"

	tests := map[string]struct {
		deliveryHandler func(any, int, chan struct{}) gorabbitmq.HandlerFunc
		getConsumer     func(*gorabbitmq.Connector, gorabbitmq.HandlerFunc, *testParams) (*gorabbitmq.Consumer, error)
	}{
		"consume with Ack": {
			deliveryHandler: func(expectedMessage any, counter int, doneChan chan struct{}) gorabbitmq.HandlerFunc {
				return func(d gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)

					doneChan <- struct{}{}

					return gorabbitmq.Ack
				}
			},
			getConsumer: func(c *gorabbitmq.Connector, handler gorabbitmq.HandlerFunc, params *testParams) (*gorabbitmq.Consumer, error) {
				return c.NewConsumerAndSubscribe(
					params.queueName,
					handler,
					gorabbitmq.WithExchangeOptionAutoDelete(true),
					gorabbitmq.WithExchangeOptionDeclare(true),
					gorabbitmq.WithExchangeOptionKind(gorabbitmq.ExchangeTopic),
					gorabbitmq.WithExchangeOptionName(params.exchangeName),
					gorabbitmq.WithExchangeOptionArgs(gorabbitmq.Table{
						"test-exchange-arg-key": "test-exchange-arg-value",
					}),
					gorabbitmq.WithQueueOptionAutoDelete(true),
					gorabbitmq.WithConsumerOptionRoutingKey(params.routingKey),
				)
			},
		},
		"consume with NackDisgard": {
			deliveryHandler: func(expectedMessage any, counter int, doneChan chan struct{}) gorabbitmq.HandlerFunc {
				return func(d gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)

					doneChan <- struct{}{}

					return gorabbitmq.NackDiscard
				}
			},
			getConsumer: func(c *gorabbitmq.Connector, handler gorabbitmq.HandlerFunc, params *testParams) (*gorabbitmq.Consumer, error) {
				return c.NewConsumerAndSubscribe(
					params.queueName,
					handler,
					gorabbitmq.WithExchangeOptionAutoDelete(true),
					gorabbitmq.WithExchangeOptionDeclare(true),
					gorabbitmq.WithExchangeOptionKind(gorabbitmq.ExchangeTopic),
					gorabbitmq.WithExchangeOptionName(params.exchangeName),
					gorabbitmq.WithQueueOptionAutoDelete(true),
					gorabbitmq.WithConsumerOptionRoutingKey(params.routingKey),
				)
			},
		},
		"consume with NackRequeue": {
			deliveryHandler: func(expectedMessage any, counter int, doneChan chan struct{}) gorabbitmq.HandlerFunc {
				return func(d gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)
					counter++

					switch counter {
					case 1:
						return gorabbitmq.NackRequeue

					case 2:
						doneChan <- struct{}{}

						return gorabbitmq.Ack
					}

					return gorabbitmq.NackDiscard
				}
			},
			getConsumer: func(c *gorabbitmq.Connector, handler gorabbitmq.HandlerFunc, params *testParams) (*gorabbitmq.Consumer, error) {
				return c.NewConsumerAndSubscribe(
					params.queueName,
					handler,
					gorabbitmq.WithExchangeOptionAutoDelete(true),
					gorabbitmq.WithExchangeOptionDeclare(true),
					gorabbitmq.WithExchangeOptionKind(gorabbitmq.ExchangeTopic),
					gorabbitmq.WithExchangeOptionName(params.exchangeName),
					gorabbitmq.WithQueueOptionAutoDelete(true),
					gorabbitmq.WithConsumerOptionRoutingKey(params.routingKey),
				)
			},
		},
		"consume with Manual": {
			deliveryHandler: func(expectedMessage any, counter int, doneChan chan struct{}) gorabbitmq.HandlerFunc {
				return func(delivery gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, expectedMessage, string(delivery.Body))
					requireEqual(t, "text/plain", delivery.ContentType)

					doneChan <- struct{}{}

					err := delivery.Ack(false)
					requireNoError(t, err)

					return gorabbitmq.Manual
				}
			},
			getConsumer: func(c *gorabbitmq.Connector, handler gorabbitmq.HandlerFunc, params *testParams) (*gorabbitmq.Consumer, error) {
				return c.NewConsumerAndSubscribe(
					params.queueName,
					handler,
					gorabbitmq.WithExchangeOptionAutoDelete(true),
					gorabbitmq.WithExchangeOptionDeclare(true),
					gorabbitmq.WithExchangeOptionKind(gorabbitmq.ExchangeTopic),
					gorabbitmq.WithExchangeOptionName(params.exchangeName),
					gorabbitmq.WithExchangeOptionArgs(gorabbitmq.Table{
						"test-exchange-arg-key": "test-exchange-arg-value",
					}),
					gorabbitmq.WithQueueOptionAutoDelete(true),
					gorabbitmq.WithConsumerOptionRoutingKey(params.routingKey),
				)
			},
		},
		"consume with AutoAck": {
			deliveryHandler: func(expectedMessage any, counter int, doneChan chan struct{}) gorabbitmq.HandlerFunc {
				return func(d gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)

					doneChan <- struct{}{}

					return gorabbitmq.Manual
				}
			},
			getConsumer: func(c *gorabbitmq.Connector, handler gorabbitmq.HandlerFunc, params *testParams) (*gorabbitmq.Consumer, error) {
				return c.NewConsumerAndSubscribe(
					params.queueName,
					handler,
					gorabbitmq.WithExchangeOptionAutoDelete(true),
					gorabbitmq.WithExchangeOptionDeclare(true),
					gorabbitmq.WithExchangeOptionKind(gorabbitmq.ExchangeTopic),
					gorabbitmq.WithExchangeOptionName(params.exchangeName),
					gorabbitmq.WithQueueOptionAutoDelete(true),
					gorabbitmq.WithConsumerOptionRoutingKey(params.routingKey),
					gorabbitmq.WithConsumerOptionConsumerAutoAck(true),
				)
			},
		},
		"consume with consumer NoWait": {
			deliveryHandler: func(expectedMessage any, counter int, doneChan chan struct{}) gorabbitmq.HandlerFunc {
				return func(d gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)

					doneChan <- struct{}{}

					return gorabbitmq.Ack
				}
			},
			getConsumer: func(c *gorabbitmq.Connector, handler gorabbitmq.HandlerFunc, params *testParams) (*gorabbitmq.Consumer, error) {
				return c.NewConsumerAndSubscribe(
					params.queueName,
					handler,
					gorabbitmq.WithExchangeOptionAutoDelete(true),
					gorabbitmq.WithExchangeOptionDeclare(true),
					gorabbitmq.WithExchangeOptionKind(gorabbitmq.ExchangeTopic),
					gorabbitmq.WithExchangeOptionName(params.exchangeName),
					gorabbitmq.WithQueueOptionAutoDelete(true),
					gorabbitmq.WithConsumerOptionNoWait(true),
					gorabbitmq.WithConsumerOptionRoutingKey(params.routingKey),
				)
			},
		},
		"consume with multiple message handlers": {
			deliveryHandler: func(expectedMessage any, counter int, doneChan chan struct{}) gorabbitmq.HandlerFunc {
				return func(d gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)

					doneChan <- struct{}{}

					return gorabbitmq.Ack
				}
			},
			getConsumer: func(c *gorabbitmq.Connector, handler gorabbitmq.HandlerFunc, params *testParams) (*gorabbitmq.Consumer, error) {
				return c.NewConsumerAndSubscribe(
					params.queueName,
					handler,
					gorabbitmq.WithExchangeOptionAutoDelete(true),
					gorabbitmq.WithExchangeOptionDeclare(true),
					gorabbitmq.WithExchangeOptionKind(gorabbitmq.ExchangeTopic),
					gorabbitmq.WithExchangeOptionName(params.exchangeName),
					gorabbitmq.WithQueueOptionAutoDelete(true),
					gorabbitmq.WithConsumerOptionHandlerQuantity(4),
					gorabbitmq.WithConsumerOptionRoutingKey(params.routingKey),
				)
			},
		},
	}

	for name, test := range tests {
		name, test := name, test

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			connector := getConnector()

			t.Cleanup(func() {
				err := connector.Close()
				requireNoError(t, err)
			})

			doneChan := make(chan struct{})

			testParams := &testParams{
				exchangeName: stringGen(),
				queueName:    stringGen(),
				routingKey:   stringGen(),
			}

			var counter int

			_, err := test.getConsumer(connector, test.deliveryHandler(message, counter, doneChan), testParams)
			requireNoError(t, err)

			publisher, err := connector.NewPublisher(
				gorabbitmq.WithPublishOptionExchange(testParams.exchangeName),
			)
			requireNoError(t, err)

			err = publisher.Publish(context.TODO(), testParams.routingKey, message)
			requireNoError(t, err)

			<-doneChan
		})
	}
}

func Test_Integration_CustomOptions(t *testing.T) {
	t.Parallel()

	message := "test-message"

	now := time.Date(2023, 8, 1, 12, 0, 0, 0, time.Local)

	tests := map[string]struct {
		connector       *gorabbitmq.Connector
		deliveryHandler func(any, *sync.WaitGroup) gorabbitmq.HandlerFunc
		getPublisher    func(*gorabbitmq.Connector) (*gorabbitmq.Publisher, error)
		publish         func(*gorabbitmq.Publisher, []string) error
	}{
		"publish with options": {
			connector: getConnector(),
			deliveryHandler: func(expectedMessage any, wg *sync.WaitGroup) gorabbitmq.HandlerFunc {
				return func(delivery gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, expectedMessage, string(delivery.Body))
					requireEqual(t, "test-service", delivery.AppId)
					requireEqual(t, "guest", delivery.UserId)
					requireEqual(t, now, delivery.Timestamp)
					requireEqual(t, "1234567890", delivery.MessageId)
					requireEqual(t, "0987654321", delivery.CorrelationId)
					requireEqual(t, "test-content-type", delivery.ContentType)
					requireEqual(t, "test-content-encoding", delivery.ContentEncoding)
					requireEqual(t, "test-type", delivery.Type)
					requireEqual(t, "20000", delivery.Expiration)
					requireEqual(t, "for-rpc-clients", delivery.ReplyTo)
					requireEqual(t, gorabbitmq.Table{"test-header": "test-value"}, gorabbitmq.Table(delivery.Headers))

					wg.Done()

					return gorabbitmq.Ack
				}
			},
			getPublisher: func(c *gorabbitmq.Connector) (*gorabbitmq.Publisher, error) {
				return c.NewPublisher(
					gorabbitmq.WithPublishOptionAppID("test-service"),
					gorabbitmq.WithPublishOptionUserID("guest"),
					gorabbitmq.WithPublishOptionTimestamp(now),
					gorabbitmq.WithPublishOptionMessageID("1234567890"),
					gorabbitmq.WithPublishOptionTracing("0987654321"),
					gorabbitmq.WithPublishOptionContentType("test-content-type"),
					gorabbitmq.WithPublishOptionContentEncoding("test-content-encoding"),
					gorabbitmq.WithPublishOptionType("test-type"),
					gorabbitmq.WithPublishOptionExpiration("20000"),
					gorabbitmq.WithPublishOptionReplyTo("for-rpc-clients"),
					gorabbitmq.WithPublishOptionHeaders(gorabbitmq.Table{
						"test-header": "test-value",
					}),
				)
			},
			publish: func(p *gorabbitmq.Publisher, targets []string) error {
				return p.PublishWithOptions(context.TODO(), targets, message)
			},
		},
		"publish with custom options": {
			connector: func() *gorabbitmq.Connector {
				amqpConfig := gorabbitmq.Config{
					Properties: amqp.Table{},
				}
				amqpConfig.Properties.SetClientConnectionName(stringGen())

				return getConnector(gorabbitmq.WithCustomConnectorOptions(
					&gorabbitmq.ConnectorOptions{
						ReturnHandler:     nil,
						Config:            &amqpConfig,
						Codec:             nil,
						PrefetchCount:     0,
						ReconnectInterval: 0,
					},
				))
			}(),
			deliveryHandler: func(expectedMessage any, wg *sync.WaitGroup) gorabbitmq.HandlerFunc {
				return func(d gorabbitmq.Delivery) gorabbitmq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)
					requireEqual(t, "messageID", d.MessageId)
					requireEqual(t, "correlationID", d.CorrelationId)
					requireEqual(t, now, d.Timestamp)

					wg.Done()

					return gorabbitmq.Ack
				}
			},
			getPublisher: func(c *gorabbitmq.Connector) (*gorabbitmq.Publisher, error) {
				return c.NewPublisher()
			},
			publish: func(p *gorabbitmq.Publisher, targets []string) error {
				return p.PublishWithOptions(
					context.TODO(),
					targets,
					message,
					gorabbitmq.WithCustomPublishOptions(
						&gorabbitmq.PublishOptions{
							MessageID:     "messageID",
							CorrelationID: "correlationID",
							Timestamp:     now,
							AppID:         "service-name",
							UserID:        "guest",
							ContentType:   "text/plain",
							Mandatory:     false,
							Headers: gorabbitmq.Table{
								"test-header": "test-header-value",
							},
							Exchange:        gorabbitmq.ExchangeDefault,
							Expiration:      "200000",
							ContentEncoding: "",
							ReplyTo:         "for-rpc-servers",
							Type:            "",
							Priority:        gorabbitmq.NoPriority,
							DeliveryMode:    gorabbitmq.TransientDelivery,
						},
					),
				)
			},
		},
	}

	for name, test := range tests {
		name, test := name, test

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			t.Cleanup(func() {
				err := test.connector.Close()
				requireNoError(t, err)
			})

			targets := []string{stringGen(), stringGen()}

			wg := &sync.WaitGroup{}

			wg.Add(2)

			_, err := test.connector.NewConsumerAndSubscribe(
				targets[0],
				test.deliveryHandler(message, wg),
				gorabbitmq.WithQueueOptionAutoDelete(true),
				gorabbitmq.WithConsumerOptionConsumerName(fmt.Sprintf("my_consumer_%s", stringGen())),
			)
			requireNoError(t, err)

			_, err = test.connector.NewConsumerAndSubscribe(targets[1], test.deliveryHandler(message, wg), gorabbitmq.WithCustomConsumeOptions(
				&gorabbitmq.ConsumeOptions{
					ConsumerOptions: &gorabbitmq.ConsumerOptions{
						Args: make(gorabbitmq.Table),
						Name: stringGen(),
					},
					QueueOptions: &gorabbitmq.QueueOptions{
						Args:       make(gorabbitmq.Table),
						AutoDelete: true,
						Declare:    true,
					},
					ExchangeOptions: &gorabbitmq.ExchangeOptions{
						Args: make(gorabbitmq.Table),
						Name: gorabbitmq.ExchangeDefault,
						Kind: amqp.ExchangeDirect,
					},
					Bindings:        []gorabbitmq.Binding{},
					HandlerQuantity: 1,
				},
			))
			requireNoError(t, err)

			publisher, err := test.getPublisher(test.connector)
			requireNoError(t, err)

			err = test.publish(publisher, targets)
			requireNoError(t, err)

			wg.Wait()
		})
	}
}

func Test_Integration_ManualRemoveExchangeQueueAndBindings(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		getConsumer func(*gorabbitmq.Connector, *testParams) (*gorabbitmq.Consumer, error)
		action      func(*gorabbitmq.Consumer, *testParams) error
	}{
		"remove queue": {
			getConsumer: func(c *gorabbitmq.Connector, params *testParams) (*gorabbitmq.Consumer, error) {
				return c.NewConsumerAndSubscribe(params.queueName, nil)
			},
			action: func(consumer *gorabbitmq.Consumer, params *testParams) error {
				removedMessages, err := consumer.RemoveQueue(params.queueName, false, false, false)
				requireNoError(t, err)

				requireEqual(t, 0, removedMessages)

				return nil
			},
		},
		"remove exchange": {
			getConsumer: func(c *gorabbitmq.Connector, params *testParams) (*gorabbitmq.Consumer, error) {
				return c.NewConsumerAndSubscribe(
					params.queueName,
					nil,
					gorabbitmq.WithExchangeOptionDeclare(true),
					gorabbitmq.WithExchangeOptionDurable(true),
					gorabbitmq.WithExchangeOptionKind(amqp.ExchangeDirect),
					gorabbitmq.WithExchangeOptionName(params.exchangeName),
					gorabbitmq.WithQueueOptionAutoDelete(true),
				)
			},
			action: func(consumer *gorabbitmq.Consumer, params *testParams) error {
				err := consumer.RemoveExchange(params.exchangeName, false, false)
				requireNoError(t, err)

				return nil
			},
		},
		"remove binding": {
			getConsumer: func(c *gorabbitmq.Connector, params *testParams) (*gorabbitmq.Consumer, error) {
				return c.NewConsumerAndSubscribe(
					params.queueName,
					nil,
					gorabbitmq.WithQueueOptionAutoDelete(true),
					gorabbitmq.WithExchangeOptionAutoDelete(true),
					gorabbitmq.WithExchangeOptionDeclare(true),
					gorabbitmq.WithExchangeOptionKind(amqp.ExchangeTopic),
					gorabbitmq.WithExchangeOptionName(params.exchangeName),
					gorabbitmq.WithConsumerOptionBinding(gorabbitmq.Binding{
						RoutingKey: params.routingKey,
						BindingOptions: &gorabbitmq.BindingOptions{
							Args:    gorabbitmq.Table{},
							NoWait:  false,
							Declare: true,
						},
					}),
				)
			},
			action: func(consumer *gorabbitmq.Consumer, params *testParams) error {
				err := consumer.RemoveBinding(params.queueName, params.routingKey, params.exchangeName, nil)
				requireNoError(t, err)

				return nil
			},
		},
	}

	for name, test := range tests {
		name, test := name, test

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			testParams := &testParams{
				exchangeName: stringGen(),
				queueName:    stringGen(),
				routingKey:   stringGen(),
			}

			connector := getConnector()

			t.Cleanup(func() {
				err := connector.Close()
				requireNoError(t, err)
			})

			consumer, err := test.getConsumer(connector, testParams)
			requireNoError(t, err)

			err = test.action(consumer, testParams)
			requireNoError(t, err)
		})
	}
}

func Test_Integration_SubscribingTwiceReturnsError(t *testing.T) {
	t.Parallel()

	connector := getConnector()

	t.Cleanup(func() {
		err := connector.Close()
		requireNoError(t, err)
	})

	consumer, err := connector.NewConsumerAndSubscribe(
		"test-queue",
		nil,
		gorabbitmq.WithQueueOptionAutoDelete(true),
	)
	requireNoError(t, err)

	err = consumer.Subscribe(nil)

	requireEqual(t, gorabbitmq.ErrAlreadySubscribed, cause(err))

	err = consumer.Unsubscribe()
	requireNoError(t, err)
}

func Test_Integration_ReturnHandler(t *testing.T) {
	t.Parallel()

	message := "test-message"

	doneChan := make(chan struct{})

	returnHandler := func(r gorabbitmq.Return) {
		requireEqual(t, message, string(r.Body))
		requireEqual(t, "text/plain", r.ContentType)

		doneChan <- struct{}{}
	}

	connector := getConnector(
		gorabbitmq.WithConnectorOptionReturnHandler(returnHandler),
		gorabbitmq.WithConnectorOptionTextLogging(os.Stdout, slog.LevelError),
		gorabbitmq.WithConnectorOptionConnectionName(stringGen()),
	)

	t.Cleanup(func() {
		err := connector.Close()
		requireNoError(t, err)
	})

	exchangeName := stringGen()
	queueName := stringGen()
	routingKey := stringGen()

	_, err := connector.NewConsumerAndSubscribe(
		queueName,
		nil,
		gorabbitmq.WithExchangeOptionDeclare(true),
		gorabbitmq.WithExchangeOptionKind(gorabbitmq.ExchangeTopic),
		gorabbitmq.WithExchangeOptionName(exchangeName),
		gorabbitmq.WithConsumerOptionRoutingKey(routingKey),
		gorabbitmq.WithQueueOptionAutoDelete(true),
		gorabbitmq.WithExchangeOptionAutoDelete(true),
	)
	requireNoError(t, err)

	publisher, err := connector.NewPublisher(
		gorabbitmq.WithPublishOptionExchange(exchangeName),
		gorabbitmq.WithPublishOptionMandatory(true),
	)
	requireNoError(t, err)

	err = publisher.Publish(context.TODO(), "does-not-exist", message)
	requireNoError(t, err)

	<-doneChan
}

func Test_DecodeDeliveryBody(t *testing.T) {
	t.Parallel()

	message := testData{
		Name:    "Name",
		Age:     157,
		City:    "City",
		Country: "Country",
	}

	jsonMessage, err := json.Marshal(&message)
	requireNoError(t, err)

	delivery := gorabbitmq.Delivery{
		Delivery: amqp.Delivery{
			ContentType: "application/json",
			Timestamp:   time.Now(),
			Body:        jsonMessage,
		},
	}

	tests := map[string]struct {
		connector *gorabbitmq.Connector
	}{
		"with standard codec": {
			connector: getConnector(),
		},
		"with self-defined codec": {
			connector: getConnector(
				gorabbitmq.WithConnectorOptionEncoder(json.Marshal),
				gorabbitmq.WithConnectorOptionDecoder(json.Unmarshal),
			),
		},
	}

	for name, test := range tests {
		name, test := name, test

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			var result testData

			err := test.connector.DecodeDeliveryBody(delivery, &result)
			requireNoError(t, err)

			requireEqual(t, message, result)
		})
	}
}

func getConnector(options ...gorabbitmq.ConnectorOption) *gorabbitmq.Connector {
	return gorabbitmq.NewConnector(&gorabbitmq.ConnectionSettings{
		UserName: "guest",
		Password: "guest",
		Host:     "localhost",
		Port:     5672,
	},
		options...,
	)
}

func requireEqual(t *testing.T, expected any, actual any) {
	t.Helper()

	equal := reflect.DeepEqual(expected, actual)

	if !equal {
		t.Errorf("Not equal: \nExpected: %v\nActual: %+v", expected, actual)
	}
}

func requireNoError(t *testing.T, err error) {
	t.Helper()

	if err != nil {
		t.Error(err)
	}
}

func cause(err error) error {
	type causer interface {
		Unwrap() error
	}

	for err != nil {
		cause, ok := err.(causer) //nolint: errorlint // expected behavior
		if !ok {
			break
		}

		err = cause.Unwrap()
	}

	return err
}

func stringGen() string {
	bytes := make([]byte, 16)

	_, err := rand.Read(bytes)
	if err != nil {
		return ""
	}

	return hex.EncodeToString(bytes)
}
