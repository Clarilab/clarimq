package clarimq_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Clarilab/clarimq"
	"github.com/Clarilab/clarimq/cache"
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
		deliveryHandler func(any, chan struct{}) clarimq.HandlerFunc
		getConsumer     func(*clarimq.Connection, clarimq.HandlerFunc, *testParams) (*clarimq.Consumer, error)
		passiveExchange bool
		message         any
	}{
		"publish to exchange / consume with exchange NoWait": {
			deliveryHandler: func(expectedMessage any, doneChan chan struct{}) clarimq.HandlerFunc {
				return func(d *clarimq.Delivery) clarimq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)

					doneChan <- struct{}{}

					return clarimq.Ack
				}
			},
			getConsumer: func(conn *clarimq.Connection, handler clarimq.HandlerFunc, params *testParams) (*clarimq.Consumer, error) {
				return clarimq.NewConsumer(
					conn,
					params.queueName,
					handler,
					clarimq.WithExchangeOptionAutoDelete(true),
					clarimq.WithExchangeOptionDeclare(true),
					clarimq.WithExchangeOptionKind(clarimq.ExchangeTopic),
					clarimq.WithExchangeOptionName(params.exchangeName),
					clarimq.WithExchangeOptionNoWait(true),
					clarimq.WithQueueOptionAutoDelete(true),
					clarimq.WithConsumerOptionRoutingKey(params.routingKey),
				)
			},

			message: stringMessage,
		},
		"publish to exchange passive": {
			deliveryHandler: func(expectedMessage any, doneChan chan struct{}) clarimq.HandlerFunc {
				return func(d *clarimq.Delivery) clarimq.Action {
					requireEqual(t, expectedMessage, d.Body)
					requireEqual(t, "application/octet-stream", d.ContentType)

					doneChan <- struct{}{}

					return clarimq.Ack
				}
			},
			getConsumer: func(conn *clarimq.Connection, handler clarimq.HandlerFunc, params *testParams) (*clarimq.Consumer, error) {
				return clarimq.NewConsumer(
					conn,
					params.queueName,
					handler,
					clarimq.WithExchangeOptionAutoDelete(true),
					clarimq.WithExchangeOptionDeclare(true),
					clarimq.WithExchangeOptionKind(clarimq.ExchangeTopic),
					clarimq.WithExchangeOptionName(params.exchangeName),
					clarimq.WithQueueOptionAutoDelete(true),
					clarimq.WithConsumerOptionRoutingKey(params.routingKey),
					clarimq.WithExchangeOptionPassive(true),
				)
			},
			passiveExchange: true,
			message:         bytesMessage,
		},
		"publish bytes message": {
			deliveryHandler: func(expectedMessage any, doneChan chan struct{}) clarimq.HandlerFunc {
				return func(d *clarimq.Delivery) clarimq.Action {
					requireEqual(t, expectedMessage, d.Body)
					requireEqual(t, "application/octet-stream", d.ContentType)

					doneChan <- struct{}{}

					return clarimq.Ack
				}
			},
			getConsumer: func(conn *clarimq.Connection, handler clarimq.HandlerFunc, params *testParams) (*clarimq.Consumer, error) {
				return clarimq.NewConsumer(
					conn,
					params.queueName,
					handler,
					clarimq.WithExchangeOptionAutoDelete(true),
					clarimq.WithExchangeOptionDeclare(true),
					clarimq.WithExchangeOptionKind(clarimq.ExchangeTopic),
					clarimq.WithExchangeOptionName(params.exchangeName),
					clarimq.WithQueueOptionAutoDelete(true),
					clarimq.WithConsumerOptionRoutingKey(params.routingKey),
				)
			},

			message: bytesMessage,
		},
		"publish json message": {
			deliveryHandler: func(expectedMessage any, doneChan chan struct{}) clarimq.HandlerFunc {
				return func(d *clarimq.Delivery) clarimq.Action {
					requireEqual(t, "application/json", d.ContentType)

					var result testData

					err := json.Unmarshal(d.Body, &result)
					requireNoError(t, err)

					requireEqual(t, expectedMessage, result)

					doneChan <- struct{}{}

					return clarimq.Ack
				}
			},
			getConsumer: func(conn *clarimq.Connection, handler clarimq.HandlerFunc, params *testParams) (*clarimq.Consumer, error) {
				return clarimq.NewConsumer(
					conn,
					params.queueName,
					handler,
					clarimq.WithExchangeOptionAutoDelete(true),
					clarimq.WithExchangeOptionDeclare(true),
					clarimq.WithExchangeOptionKind(clarimq.ExchangeTopic),
					clarimq.WithExchangeOptionName(params.exchangeName),
					clarimq.WithQueueOptionAutoDelete(true),
					clarimq.WithConsumerOptionRoutingKey(params.routingKey),
				)
			},
			message: jsonMessage,
		},
	}

	for name, test := range tests {
		name, test := name, test

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			publishConn := getConnection(t)
			consumerConn := getConnection(t)

			t.Cleanup(func() {
				err := publishConn.Close()
				requireNoError(t, err)

				err = consumerConn.Close()
				requireNoError(t, err)
			})

			doneChan := make(chan struct{})

			testParams := &testParams{
				exchangeName: stringGen(),
				queueName:    stringGen(),
				routingKey:   stringGen(),
			}

			// connecting to a passive exchange requires the exchange to exist beforehand
			// so here the exchange gets declared before the binding is declared.

			if test.passiveExchange {
				consumer, err := clarimq.NewConsumer(
					consumerConn,
					testParams.queueName,
					nil,
					clarimq.WithExchangeOptionDeclare(true),
					clarimq.WithExchangeOptionKind(clarimq.ExchangeTopic),
					clarimq.WithExchangeOptionName(testParams.exchangeName),
					clarimq.WithQueueOptionAutoDelete(true),
					clarimq.WithConsumerOptionRoutingKey(testParams.routingKey),
				)

				requireNoError(t, err)

				err = consumer.Close()
				requireNoError(t, err)
			}

			_, err := test.getConsumer(consumerConn, test.deliveryHandler(test.message, doneChan), testParams)
			requireNoError(t, err)

			publisher, err := clarimq.NewPublisher(
				publishConn,
				clarimq.WithPublishOptionExchange(testParams.exchangeName),
			)
			requireNoError(t, err)

			err = publisher.Publish(context.Background(), testParams.routingKey, test.message)
			requireNoError(t, err)

			<-doneChan

			// cleaning up the passive exchange again
			if test.passiveExchange {
				err = consumerConn.RemoveExchange(testParams.exchangeName, false, false)
				requireNoError(t, err)
			}
		})
	}
}

func Test_Integration_PublishToQueue(t *testing.T) {
	t.Parallel()

	message := "test-message"

	tests := map[string]struct {
		deliveryHandler func(any, chan struct{}) clarimq.HandlerFunc
		getConsumer     func(*clarimq.Connection, clarimq.HandlerFunc, string) (*clarimq.Consumer, error)
		passiveQueue    bool
		publish         func(*clarimq.Publisher, string) error
	}{
		"publish to queue": {
			deliveryHandler: func(expectedMessage any, doneChan chan struct{}) clarimq.HandlerFunc {
				return func(d *clarimq.Delivery) clarimq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)

					doneChan <- struct{}{}

					return clarimq.Ack
				}
			},
			getConsumer: func(conn *clarimq.Connection, handler clarimq.HandlerFunc, queueName string) (*clarimq.Consumer, error) {
				return clarimq.NewConsumer(
					conn,
					queueName,
					handler,
					clarimq.WithQueueOptionAutoDelete(true),
					clarimq.WithQueueOptionArgs(clarimq.Table{
						"test-queue-arg-key": "test-queue-arg-value",
					}),
				)
			},
			publish: func(p *clarimq.Publisher, target string) error {
				return p.PublishWithOptions(context.Background(), []string{target}, message)
			},
		},
		"publish to queue passive": {
			deliveryHandler: func(expectedMessage any, doneChan chan struct{}) clarimq.HandlerFunc {
				return func(d *clarimq.Delivery) clarimq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)

					doneChan <- struct{}{}

					return clarimq.Ack
				}
			},
			getConsumer: func(conn *clarimq.Connection, handler clarimq.HandlerFunc, queueName string) (*clarimq.Consumer, error) {
				return clarimq.NewConsumer(
					conn,
					queueName,
					handler,
					clarimq.WithQueueOptionAutoDelete(true),
					clarimq.WithQueueOptionPassive(true),
				)
			},
			publish: func(p *clarimq.Publisher, target string) error {
				return p.PublishWithOptions(context.Background(), []string{target}, message)
			},
			passiveQueue: true,
		},
		"publish to queue NoWait": {
			deliveryHandler: func(expectedMessage any, doneChan chan struct{}) clarimq.HandlerFunc {
				return func(d *clarimq.Delivery) clarimq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)

					doneChan <- struct{}{}

					return clarimq.Ack
				}
			},
			getConsumer: func(conn *clarimq.Connection, handler clarimq.HandlerFunc, queueName string) (*clarimq.Consumer, error) {
				return clarimq.NewConsumer(
					conn,
					queueName,
					handler,
					clarimq.WithQueueOptionAutoDelete(true),
					clarimq.WithQueueOptionNoWait(true),
				)
			},
			publish: func(p *clarimq.Publisher, target string) error {
				return p.PublishWithOptions(context.Background(), []string{target}, message)
			},
		},
		"publish to priority queue": {
			deliveryHandler: func(expectedMessage any, doneChan chan struct{}) clarimq.HandlerFunc {
				return func(d *clarimq.Delivery) clarimq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)
					requireEqual(t, 4, int(d.Priority))

					doneChan <- struct{}{}

					return clarimq.Ack
				}
			},
			getConsumer: func(conn *clarimq.Connection, handler clarimq.HandlerFunc, queueName string) (*clarimq.Consumer, error) {
				return clarimq.NewConsumer(
					conn,
					queueName,
					handler,
					clarimq.WithQueueOptionAutoDelete(true),
					clarimq.WithQueueOptionPriority(clarimq.HighestPriority),
				)
			},
			publish: func(p *clarimq.Publisher, target string) error {
				return p.PublishWithOptions(context.Background(), []string{target}, message, clarimq.WithPublishOptionPriority(clarimq.HighPriority))
			},
		},
		"publish to durable queue": {
			deliveryHandler: func(expectedMessage any, doneChan chan struct{}) clarimq.HandlerFunc {
				return func(d *clarimq.Delivery) clarimq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)

					doneChan <- struct{}{}

					return clarimq.Ack
				}
			},
			getConsumer: func(conn *clarimq.Connection, handler clarimq.HandlerFunc, queueName string) (*clarimq.Consumer, error) {
				return clarimq.NewConsumer(
					conn,
					queueName,
					handler,
					clarimq.WithQueueOptionAutoDelete(true),
					clarimq.WithQueueOptionDurable(true),
				)
			},
			publish: func(p *clarimq.Publisher, target string) error {
				return p.PublishWithOptions(context.Background(), []string{target}, message)
			},
		},
	}

	for name, test := range tests {
		name, test := name, test

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			publishConn := getConnection(t)
			consumeConn := getConnection(t)

			t.Cleanup(func() {
				err := publishConn.Close()
				requireNoError(t, err)

				err = consumeConn.Close()
				requireNoError(t, err)
			})

			doneChan := make(chan struct{})
			queueName := stringGen()

			// connecting to a passive queue requires the queue to exist beforehand
			// so here the queue gets declared before the consumer subscribes.

			if test.passiveQueue {
				consumer, err := clarimq.NewConsumer(consumeConn, queueName, nil)

				requireNoError(t, err)

				err = consumer.Close()
				requireNoError(t, err)
			}

			_, err := test.getConsumer(consumeConn, test.deliveryHandler(message, doneChan), queueName)
			requireNoError(t, err)

			publisher, err := clarimq.NewPublisher(publishConn)
			requireNoError(t, err)

			err = test.publish(publisher, queueName)
			requireNoError(t, err)

			<-doneChan

			// cleaning up the passive queue again
			if test.passiveQueue {
				_, err = consumeConn.RemoveQueue(queueName, false, false, false)
				requireNoError(t, err)
			}
		})
	}
}

func Test_Integration_Consume(t *testing.T) {
	t.Parallel()

	message := "test-message"

	tests := map[string]struct {
		deliveryHandler func(any, int, chan struct{}) clarimq.HandlerFunc
		getConsumer     func(*clarimq.Connection, clarimq.HandlerFunc, *testParams) (*clarimq.Consumer, error)
	}{
		"consume with Ack": {
			deliveryHandler: func(expectedMessage any, counter int, doneChan chan struct{}) clarimq.HandlerFunc {
				return func(d *clarimq.Delivery) clarimq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)

					doneChan <- struct{}{}

					return clarimq.Ack
				}
			},
			getConsumer: func(conn *clarimq.Connection, handler clarimq.HandlerFunc, params *testParams) (*clarimq.Consumer, error) {
				return clarimq.NewConsumer(
					conn,
					params.queueName,
					handler,
					clarimq.WithExchangeOptionAutoDelete(true),
					clarimq.WithExchangeOptionDeclare(true),
					clarimq.WithExchangeOptionKind(clarimq.ExchangeTopic),
					clarimq.WithExchangeOptionName(params.exchangeName),
					clarimq.WithExchangeOptionArgs(clarimq.Table{
						"test-exchange-arg-key": "test-exchange-arg-value",
					}),
					clarimq.WithQueueOptionAutoDelete(true),
					clarimq.WithConsumerOptionRoutingKey(params.routingKey),
				)
			},
		},
		"consume with NackDisgard": {
			deliveryHandler: func(expectedMessage any, counter int, doneChan chan struct{}) clarimq.HandlerFunc {
				return func(d *clarimq.Delivery) clarimq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)

					doneChan <- struct{}{}

					return clarimq.NackDiscard
				}
			},
			getConsumer: func(conn *clarimq.Connection, handler clarimq.HandlerFunc, params *testParams) (*clarimq.Consumer, error) {
				return clarimq.NewConsumer(
					conn,
					params.queueName,
					handler,
					clarimq.WithExchangeOptionAutoDelete(true),
					clarimq.WithExchangeOptionDeclare(true),
					clarimq.WithExchangeOptionKind(clarimq.ExchangeTopic),
					clarimq.WithExchangeOptionName(params.exchangeName),
					clarimq.WithQueueOptionAutoDelete(true),
					clarimq.WithConsumerOptionRoutingKey(params.routingKey),
				)
			},
		},
		"consume with NackRequeue": {
			deliveryHandler: func(expectedMessage any, counter int, doneChan chan struct{}) clarimq.HandlerFunc {
				return func(d *clarimq.Delivery) clarimq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)
					counter++

					switch counter {
					case 1:
						return clarimq.NackRequeue

					case 2:
						doneChan <- struct{}{}

						return clarimq.Ack
					}

					return clarimq.NackDiscard
				}
			},
			getConsumer: func(conn *clarimq.Connection, handler clarimq.HandlerFunc, params *testParams) (*clarimq.Consumer, error) {
				return clarimq.NewConsumer(
					conn,
					params.queueName,
					handler,
					clarimq.WithExchangeOptionAutoDelete(true),
					clarimq.WithExchangeOptionDeclare(true),
					clarimq.WithExchangeOptionKind(clarimq.ExchangeTopic),
					clarimq.WithExchangeOptionName(params.exchangeName),
					clarimq.WithQueueOptionAutoDelete(true),
					clarimq.WithConsumerOptionRoutingKey(params.routingKey),
				)
			},
		},
		"consume with Manual": {
			deliveryHandler: func(expectedMessage any, counter int, doneChan chan struct{}) clarimq.HandlerFunc {
				return func(delivery *clarimq.Delivery) clarimq.Action {
					requireEqual(t, expectedMessage, string(delivery.Body))
					requireEqual(t, "text/plain", delivery.ContentType)

					doneChan <- struct{}{}

					err := delivery.Ack(false)
					requireNoError(t, err)

					return clarimq.Manual
				}
			},
			getConsumer: func(conn *clarimq.Connection, handler clarimq.HandlerFunc, params *testParams) (*clarimq.Consumer, error) {
				return clarimq.NewConsumer(
					conn,
					params.queueName,
					handler,
					clarimq.WithExchangeOptionAutoDelete(true),
					clarimq.WithExchangeOptionDeclare(true),
					clarimq.WithExchangeOptionKind(clarimq.ExchangeTopic),
					clarimq.WithExchangeOptionName(params.exchangeName),
					clarimq.WithExchangeOptionArgs(clarimq.Table{
						"test-exchange-arg-key": "test-exchange-arg-value",
					}),
					clarimq.WithQueueOptionAutoDelete(true),
					clarimq.WithConsumerOptionRoutingKey(params.routingKey),
				)
			},
		},
		"consume with AutoAck": {
			deliveryHandler: func(expectedMessage any, counter int, doneChan chan struct{}) clarimq.HandlerFunc {
				return func(d *clarimq.Delivery) clarimq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)

					doneChan <- struct{}{}

					return clarimq.Manual
				}
			},
			getConsumer: func(conn *clarimq.Connection, handler clarimq.HandlerFunc, params *testParams) (*clarimq.Consumer, error) {
				return clarimq.NewConsumer(
					conn,
					params.queueName,
					handler,
					clarimq.WithExchangeOptionAutoDelete(true),
					clarimq.WithExchangeOptionDeclare(true),
					clarimq.WithExchangeOptionKind(clarimq.ExchangeTopic),
					clarimq.WithExchangeOptionName(params.exchangeName),
					clarimq.WithQueueOptionAutoDelete(true),
					clarimq.WithConsumerOptionRoutingKey(params.routingKey),
					clarimq.WithConsumerOptionConsumerAutoAck(true),
				)
			},
		},
		"consume with consumer NoWait": {
			deliveryHandler: func(expectedMessage any, counter int, doneChan chan struct{}) clarimq.HandlerFunc {
				return func(d *clarimq.Delivery) clarimq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)

					doneChan <- struct{}{}

					return clarimq.Ack
				}
			},
			getConsumer: func(conn *clarimq.Connection, handler clarimq.HandlerFunc, params *testParams) (*clarimq.Consumer, error) {
				return clarimq.NewConsumer(
					conn,
					params.queueName,
					handler,
					clarimq.WithExchangeOptionAutoDelete(true),
					clarimq.WithExchangeOptionDeclare(true),
					clarimq.WithExchangeOptionKind(clarimq.ExchangeTopic),
					clarimq.WithExchangeOptionName(params.exchangeName),
					clarimq.WithQueueOptionAutoDelete(true),
					clarimq.WithConsumerOptionNoWait(true),
					clarimq.WithConsumerOptionRoutingKey(params.routingKey),
				)
			},
		},
		"consume with multiple message handlers": {
			deliveryHandler: func(expectedMessage any, counter int, doneChan chan struct{}) clarimq.HandlerFunc {
				return func(d *clarimq.Delivery) clarimq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)

					doneChan <- struct{}{}

					return clarimq.Ack
				}
			},
			getConsumer: func(conn *clarimq.Connection, handler clarimq.HandlerFunc, params *testParams) (*clarimq.Consumer, error) {
				return clarimq.NewConsumer(
					conn,
					params.queueName,
					handler,
					clarimq.WithExchangeOptionAutoDelete(true),
					clarimq.WithExchangeOptionDeclare(true),
					clarimq.WithExchangeOptionKind(clarimq.ExchangeTopic),
					clarimq.WithExchangeOptionName(params.exchangeName),
					clarimq.WithQueueOptionAutoDelete(true),
					clarimq.WithConsumerOptionHandlerQuantity(4),
					clarimq.WithConsumerOptionRoutingKey(params.routingKey),
				)
			},
		},
	}

	for name, test := range tests {
		name, test := name, test

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			publishConn := getConnection(t)
			consumeConn := getConnection(t)

			t.Cleanup(func() {
				err := publishConn.Close()
				requireNoError(t, err)

				err = consumeConn.Close()
				requireNoError(t, err)
			})

			doneChan := make(chan struct{})

			testParams := &testParams{
				exchangeName: stringGen(),
				queueName:    stringGen(),
				routingKey:   stringGen(),
			}

			var counter int

			_, err := test.getConsumer(consumeConn, test.deliveryHandler(message, counter, doneChan), testParams)
			requireNoError(t, err)

			publisher, err := clarimq.NewPublisher(
				publishConn,
				clarimq.WithPublishOptionExchange(testParams.exchangeName),
			)
			requireNoError(t, err)

			err = publisher.Publish(context.Background(), testParams.routingKey, message)
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
		publishConn     *clarimq.Connection
		deliveryHandler func(any, *sync.WaitGroup) clarimq.HandlerFunc
		getPublisher    func(*clarimq.Connection) (*clarimq.Publisher, error)
		publish         func(*clarimq.Publisher, []string) error
	}{
		"publish with options": {
			publishConn: getConnection(t),
			deliveryHandler: func(expectedMessage any, wg *sync.WaitGroup) clarimq.HandlerFunc {
				return func(delivery *clarimq.Delivery) clarimq.Action {
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
					requireEqual(t, clarimq.Table{"test-header": "test-value"}, clarimq.Table(delivery.Headers))

					wg.Done()

					return clarimq.Ack
				}
			},
			getPublisher: func(conn *clarimq.Connection) (*clarimq.Publisher, error) {
				return clarimq.NewPublisher(
					conn,
					clarimq.WithPublishOptionAppID("test-service"),
					clarimq.WithPublishOptionUserID("guest"),
					clarimq.WithPublishOptionTimestamp(now),
					clarimq.WithPublishOptionMessageID("1234567890"),
					clarimq.WithPublishOptionTracing("0987654321"),
					clarimq.WithPublishOptionContentType("test-content-type"),
					clarimq.WithPublishOptionContentEncoding("test-content-encoding"),
					clarimq.WithPublishOptionType("test-type"),
					clarimq.WithPublishOptionExpiration("20000"),
					clarimq.WithPublishOptionReplyTo("for-rpc-clients"),
					clarimq.WithPublishOptionHeaders(clarimq.Table{
						"test-header": "test-value",
					}),
				)
			},
			publish: func(p *clarimq.Publisher, targets []string) error {
				return p.PublishWithOptions(context.Background(), targets, message)
			},
		},
		"publish with custom options": {
			publishConn: func() *clarimq.Connection {
				amqpConfig := clarimq.Config{
					Properties: amqp.Table{},
				}
				amqpConfig.Properties.SetClientConnectionName(stringGen())

				return getConnection(t, clarimq.WithCustomConnectionOptions(
					&clarimq.ConnectionOptions{
						ReturnHandler:    nil,
						Config:           &amqpConfig,
						PrefetchCount:    0,
						RecoveryInterval: 0,
					},
				))
			}(),
			deliveryHandler: func(expectedMessage any, wg *sync.WaitGroup) clarimq.HandlerFunc {
				return func(d *clarimq.Delivery) clarimq.Action {
					requireEqual(t, expectedMessage, string(d.Body))
					requireEqual(t, "text/plain", d.ContentType)
					requireEqual(t, "messageID", d.MessageId)
					requireEqual(t, "correlationID", d.CorrelationId)
					requireEqual(t, now, d.Timestamp)

					wg.Done()

					return clarimq.Ack
				}
			},
			getPublisher: func(conn *clarimq.Connection) (*clarimq.Publisher, error) {
				return clarimq.NewPublisher(conn)
			},
			publish: func(p *clarimq.Publisher, targets []string) error {
				return p.PublishWithOptions(
					context.Background(),
					targets,
					message,
					clarimq.WithCustomPublishOptions(
						&clarimq.PublisherOptions{
							PublishingOptions: &clarimq.PublishOptions{
								MessageID:     "messageID",
								CorrelationID: "correlationID",
								Timestamp:     now,
								AppID:         "service-name",
								UserID:        "guest",
								ContentType:   "text/plain",
								Mandatory:     false,
								Headers: clarimq.Table{
									"test-header": "test-header-value",
								},
								Exchange:        clarimq.ExchangeDefault,
								Expiration:      "200000",
								ContentEncoding: "",
								ReplyTo:         "for-rpc-servers",
								Type:            "",
								Priority:        clarimq.NoPriority,
								DeliveryMode:    clarimq.TransientDelivery,
							},
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

			targets := []string{stringGen(), stringGen()}

			consumeConn := getConnection(t)

			t.Cleanup(func() {
				err := test.publishConn.Close()
				requireNoError(t, err)

				err = consumeConn.Close()
				requireNoError(t, err)
			})

			wg := &sync.WaitGroup{}

			// adding 2 to wait for both consumers to handle their deliveries.
			wg.Add(2)

			// registering first consumer.
			_, err := clarimq.NewConsumer(
				consumeConn,
				targets[0],
				test.deliveryHandler(message, wg),
				clarimq.WithQueueOptionAutoDelete(true),
				clarimq.WithConsumerOptionConsumerName(fmt.Sprintf("my_consumer_%s", stringGen())),
			)
			requireNoError(t, err)

			// registering second consumer with custom options.
			_, err = clarimq.NewConsumer(
				consumeConn,
				targets[1],
				test.deliveryHandler(message, wg),
				clarimq.WithCustomConsumeOptions(
					&clarimq.ConsumeOptions{
						ConsumerOptions: &clarimq.ConsumerOptions{
							Args: make(clarimq.Table),
							Name: stringGen(),
						},
						QueueOptions: &clarimq.QueueOptions{
							Args:       make(clarimq.Table),
							AutoDelete: true,
							Declare:    true,
						},
						ExchangeOptions: &clarimq.ExchangeOptions{
							Args: make(clarimq.Table),
							Name: clarimq.ExchangeDefault,
							Kind: amqp.ExchangeDirect,
						},
						Bindings:        []clarimq.Binding{},
						HandlerQuantity: 1,
					},
				),
			)
			requireNoError(t, err)

			publisher, err := test.getPublisher(test.publishConn)
			requireNoError(t, err)

			// publishing to multiple targets
			err = test.publish(publisher, targets)
			requireNoError(t, err)

			wg.Wait()
		})
	}
}

func Test_Integration_ManualRemoveExchangeQueueAndBindings(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		getConsumer func(*clarimq.Connection, *testParams) (*clarimq.Consumer, error)
		action      func(*clarimq.Connection, *testParams) error
	}{
		"remove queue": {
			getConsumer: func(conn *clarimq.Connection, params *testParams) (*clarimq.Consumer, error) {
				return clarimq.NewConsumer(conn, params.queueName, nil)
			},
			action: func(conn *clarimq.Connection, params *testParams) error {
				removedMessages, err := conn.RemoveQueue(params.queueName, false, false, false)
				requireNoError(t, err)

				requireEqual(t, 0, removedMessages)

				return nil
			},
		},
		"remove exchange": {
			getConsumer: func(conn *clarimq.Connection, params *testParams) (*clarimq.Consumer, error) {
				return clarimq.NewConsumer(
					conn,
					params.queueName,
					nil,
					clarimq.WithExchangeOptionDeclare(true),
					clarimq.WithExchangeOptionDurable(true),
					clarimq.WithExchangeOptionKind(amqp.ExchangeDirect),
					clarimq.WithExchangeOptionName(params.exchangeName),
					clarimq.WithQueueOptionAutoDelete(true),
				)
			},
			action: func(conn *clarimq.Connection, params *testParams) error {
				err := conn.RemoveExchange(params.exchangeName, false, false)
				requireNoError(t, err)

				return nil
			},
		},
		"remove binding": {
			getConsumer: func(conn *clarimq.Connection, params *testParams) (*clarimq.Consumer, error) {
				return clarimq.NewConsumer(
					conn,
					params.queueName,
					nil,
					clarimq.WithQueueOptionAutoDelete(true),
					clarimq.WithExchangeOptionAutoDelete(true),
					clarimq.WithExchangeOptionDeclare(true),
					clarimq.WithExchangeOptionKind(amqp.ExchangeTopic),
					clarimq.WithExchangeOptionName(params.exchangeName),
					clarimq.WithBindingOptionCustomBinding(clarimq.Binding{
						RoutingKey: params.routingKey,
						BindingOptions: &clarimq.BindingOptions{
							Args:    clarimq.Table{},
							NoWait:  false,
							Declare: true,
						},
					}),
				)
			},
			action: func(conn *clarimq.Connection, params *testParams) error {
				err := conn.RemoveBinding(params.queueName, params.routingKey, params.exchangeName, nil)
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

			conn := getConnection(t)

			t.Cleanup(func() {
				err := conn.Close()
				requireNoError(t, err)
			})

			_, err := test.getConsumer(conn, testParams)
			requireNoError(t, err)

			err = test.action(conn, testParams)
			requireNoError(t, err)
		})
	}
}

func Test_Integration_ReturnHandler(t *testing.T) {
	t.Parallel()

	message := "test-message"

	doneChan := make(chan struct{})

	returnHandler := func(r clarimq.Return) {
		requireEqual(t, message, string(r.Body))
		requireEqual(t, "text/plain", r.ContentType)

		doneChan <- struct{}{}
	}

	publishConn := getConnection(
		t,
		clarimq.WithConnectionOptionReturnHandler(returnHandler),
		clarimq.WithConnectionOptionTextLogging(os.Stdout, slog.LevelError),
		clarimq.WithConnectionOptionConnectionName(stringGen()),
	)

	consumerConn := getConnection(t)

	t.Cleanup(func() {
		err := publishConn.Close()
		requireNoError(t, err)

		err = consumerConn.Close()
		requireNoError(t, err)
	})

	exchangeName := stringGen()
	queueName := stringGen()
	routingKey := stringGen()

	_, err := clarimq.NewConsumer(
		consumerConn,
		queueName,
		nil,
		clarimq.WithExchangeOptionDeclare(true),
		clarimq.WithExchangeOptionKind(clarimq.ExchangeTopic),
		clarimq.WithExchangeOptionName(exchangeName),
		clarimq.WithConsumerOptionRoutingKey(routingKey),
		clarimq.WithQueueOptionAutoDelete(true),
		clarimq.WithExchangeOptionAutoDelete(true),
	)
	requireNoError(t, err)

	publisher, err := clarimq.NewPublisher(
		publishConn,
		clarimq.WithPublishOptionExchange(exchangeName),
		clarimq.WithPublishOptionMandatory(true),
	)
	requireNoError(t, err)

	// publishing a mandatory message with a routing key with out the existence of a binding.
	err = publisher.Publish(context.Background(), "does-not-exist", message)
	requireNoError(t, err)

	// the publishing is returned to the return handler.

	// waiting for the return handler to process the message.
	<-doneChan
}

func Test_Integration_DecodeDeliveryBody(t *testing.T) {
	t.Parallel()

	message := testData{
		Name:    "Name",
		Age:     157,
		City:    "City",
		Country: "Country",
	}

	jsonMessage, err := json.Marshal(&message)
	requireNoError(t, err)

	delivery := clarimq.Delivery{
		Delivery: amqp.Delivery{
			ContentType: "application/json",
			Timestamp:   time.Now(),
			Body:        jsonMessage,
		},
	}

	tests := map[string]struct {
		conn *clarimq.Connection
	}{
		"with standard codec": {
			conn: getConnection(t),
		},
		"with self-defined codec": {
			conn: getConnection(
				t,
				clarimq.WithConnectionOptionEncoder(json.Marshal),
				clarimq.WithConnectionOptionDecoder(json.Unmarshal),
			),
		},
	}

	for name, test := range tests {
		name, test := name, test

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			t.Cleanup(func() {
				err := test.conn.Close()
				requireNoError(t, err)
			})

			var result testData

			err = test.conn.DecodeDeliveryBody(delivery, &result)
			requireNoError(t, err)

			requireEqual(t, message, result)
		})
	}
}

func Test_Integration_DeadLetterRetry(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		conn *clarimq.Connection
	}{
		"with provided connection": {
			conn: getConnection(t),
		},
		"without provided connection": {
			conn: nil,
		},
	}

	for name, test := range tests {
		name, test := name, test

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			testMessage := stringGen()
			exchangeName := stringGen()
			queueName := stringGen()
			routingKey := stringGen()

			publishConn := getConnection(t)
			consumeConn := getConnection(t)

			t.Cleanup(func() {
				err := publishConn.Close()
				requireNoError(t, err)

				err = consumeConn.Close()
				requireNoError(t, err)
			})

			publisher, err := clarimq.NewPublisher(publishConn,
				clarimq.WithPublishOptionExchange(exchangeName),
			)
			requireNoError(t, err)

			doneChan := make(chan struct{})

			handler := func(delivery *clarimq.Delivery) clarimq.Action {
				requireEqual(t, testMessage, string(delivery.Body))

				retryCount, _ := delivery.Headers["x-retry-count"].(int32) //nolint:revive // test code

				if retryCount < 2 {
					return clarimq.NackDiscard
				}

				doneChan <- struct{}{}

				return clarimq.Ack
			}

			consumer, err := clarimq.NewConsumer(consumeConn, queueName, handler,
				clarimq.WithExchangeOptionDeclare(true),
				clarimq.WithExchangeOptionName(exchangeName),
				clarimq.WithExchangeOptionName(exchangeName),
				clarimq.WithExchangeOptionAutoDelete(true),
				clarimq.WithConsumerOptionRoutingKey(routingKey),
				clarimq.WithQueueOptionAutoDelete(true),
				clarimq.WithConsumerOptionDeadLetterRetry(
					&clarimq.RetryOptions{
						RetryConn: test.conn,
						Delays: []time.Duration{
							time.Second,
							time.Second * 2,
							time.Second * 3,
							time.Second * 4,
							time.Second * 5,
						},
						MaxRetries: 5,
						Cleanup:    true,
					},
				),
			)
			requireNoError(t, err)

			err = publisher.Publish(context.Background(), routingKey, testMessage)
			requireNoError(t, err)

			<-doneChan

			err = consumer.Close()
			requireNoError(t, err)
		})
	}
}

func Test_Integration_ConnectionName(t *testing.T) {
	t.Parallel()

	t.Run("connection name is set", func(t *testing.T) {
		t.Parallel()

		conn := getConnection(t, clarimq.WithConnectionOptionConnectionName("connection-name"))
		t.Cleanup(func() {
			if err := conn.Close(); err != nil {
				t.Error(err)
			}
		})

		if conn.Name() != "connection-name" {
			t.Errorf("expected connection name to be 'connection-name', got: '%s'", conn.Name())
		}
	})

	t.Run("connection name is not set", func(t *testing.T) {
		t.Parallel()

		conn := getConnection(t)
		t.Cleanup(func() {
			if err := conn.Close(); err != nil {
				t.Error(err)
			}
		})

		if !strings.Contains(conn.Name(), "connection_") {
			t.Errorf("expected connection name to contain 'connection_', got: '%s'", conn.Name())
		}
	})
}

// testBuffer is used as buffer for the logging io.Writer
// with mutex protection for concurrent access.
type testBuffer struct {
	mtx  *sync.Mutex
	buff *bytes.Buffer
}

// Write implements io.Writer interface.
// Calls the underlying bytes.Buffer method with mutex protection.
func (tb *testBuffer) Write(p []byte) (int, error) {
	tb.mtx.Lock()
	defer tb.mtx.Unlock()

	return tb.buff.Write(p)
}

// ReadBytes calls the underlying bytes.Buffer method with mutex protection.
func (tb *testBuffer) ReadBytes(delim byte) ([]byte, error) {
	tb.mtx.Lock()
	defer tb.mtx.Unlock()

	return tb.buff.ReadBytes(delim)
}

// ReadBytes calls the underlying bytes.Buffer method with mutex protection.
func (tb *testBuffer) Reset() {
	tb.mtx.Lock()
	defer tb.mtx.Unlock()

	tb.buff.Reset()
}

// ReadBytes calls the underlying bytes.Buffer method with mutex protection.
func (tb *testBuffer) Len() int {
	tb.mtx.Lock()
	defer tb.mtx.Unlock()

	return tb.buff.Len()
}

// logEntry is the log entry that will be written to the buffer.
type logEntry struct {
	Time  time.Time `json:"time"`
	Level string    `json:"level"`
	Msg   string    `json:"msg"`
}

func Test_Recovery_AutomaticRecovery(t *testing.T) { //nolint:paralleltest // intentional: must not run in parallel
	// used to wait until the handler processed the deliveries.
	doneChan := make(chan struct{})

	message := "test-message"

	// declaring a mutex protected publishConnLogBuffer.
	publishConnLogBuffer := &testBuffer{
		mtx:  &sync.Mutex{},
		buff: new(bytes.Buffer),
	}

	// declaring a mutex protected consumeConnLogBuffer.
	consumeConnLogBuffer := &testBuffer{
		mtx:  &sync.Mutex{},
		buff: new(bytes.Buffer),
	}

	// declaring the connections with JSON logging on debug level enabled.
	// (later used to compare if the recovery was successful).
	publishConn := getConnection(t,
		clarimq.WithConnectionOptionJSONLogging(publishConnLogBuffer, slog.LevelDebug),
		clarimq.WithConnectionOptionBackOffFactor(1),
		clarimq.WithConnectionOptionRecoveryInterval(500*time.Millisecond),
	)

	consumeConn := getConnection(t,
		clarimq.WithConnectionOptionJSONLogging(consumeConnLogBuffer, slog.LevelDebug),
		clarimq.WithConnectionOptionBackOffFactor(1),
		clarimq.WithConnectionOptionRecoveryInterval(500*time.Millisecond),
	)

	t.Cleanup(func() {
		err := publishConn.Close()
		requireNoError(t, err)

		err = consumeConn.Close()
		requireNoError(t, err)
	})

	// msgCounter is used to count the number of deliveries, to compare it afterwords.
	var msgCounter int

	handler := func(msg *clarimq.Delivery) clarimq.Action {
		requireEqual(t, message, string(msg.Body))

		msgCounter++

		doneChan <- struct{}{}

		return clarimq.Ack
	}

	queueName := stringGen()

	// creating a consumer.
	_, err := clarimq.NewConsumer(consumeConn, queueName, handler,
		clarimq.WithQueueOptionDurable(true),
		clarimq.WithConsumerOptionConsumerName(stringGen()),
	)
	requireNoError(t, err)

	// creating a publisher.
	publisher, err := clarimq.NewPublisher(publishConn)
	requireNoError(t, err)

	// publish a message.
	err = publisher.Publish(context.Background(), queueName, message)
	requireNoError(t, err)

	// waiting for the handler to process the delivery.
	<-doneChan

	// comparing the msgCounter that should be incremented by the consumer handler.
	requireEqual(t, 1, msgCounter)

	// shutting down the rabbitmq container to simulate a connection loss.
	err = exec.Command("docker", "compose", "stop", "rabbitmq").Run()
	requireNoError(t, err)

	// bringing the rabbitmq container up again.
	err = exec.Command("docker", "compose", "up", "-d").Run()
	requireNoError(t, err)

	// While trying to recover, the logger writes information about the recovery state on debug level.
	// In the following routines, the buffer given to the logger is read until the msg in the
	// log-entry states that the recovery was successful.

	wg := &sync.WaitGroup{}

	wg.Add(4)

	// reading the logs
	go watchConnLogBuffer(publishConnLogBuffer, wg)
	go watchConnLogBuffer(consumeConnLogBuffer, wg)

	// waiting for the both connections to be successfully recovered.
	wg.Wait()

	// publish a new message to the queue after the recovery.
	err = publisher.Publish(context.Background(), queueName, message)
	requireNoError(t, err)

	// waiting for the recovered consumer to process the new message.
	<-doneChan

	// comparing the msgCounter again that should now be incremented by the consumer handler to 2.
	requireEqual(t, 2, msgCounter)
}

func watchConnLogBuffer(buffer *testBuffer, wg *sync.WaitGroup) {
	for {
		line, err := buffer.ReadBytes('\n')
		if errors.Is(err, io.EOF) {
			continue
		}

		var logEntry logEntry

		_ = json.Unmarshal(line, &logEntry)

		if buffer.Len() == 0 {
			buffer.Reset()
		}

		switch logEntry.Msg {
		case "successfully recovered connection":
			wg.Done()

			continue
		case "successfully recovered channel":
			wg.Done()

			return
		}
	}
}

func Test_Recovery_AutomaticRecoveryFailedTryManualRecovery(t *testing.T) { //nolint:paralleltest // intentional: must not run in parallel
	// used to wait until the handler processed the deliveries.
	doneChan := make(chan struct{})

	message := "test-message"

	// declaring the connections with a maximum of 1 recovery attempts.
	publishConn := getConnection(t,
		clarimq.WithConnectionOptionMaxRecoveryRetries(4),
		clarimq.WithConnectionOptionBackOffFactor(1),
	)

	consumeConn := getConnection(t,
		clarimq.WithConnectionOptionMaxRecoveryRetries(4),
		clarimq.WithConnectionOptionBackOffFactor(1),
	)

	t.Cleanup(func() {
		err := publishConn.Close()
		requireNoError(t, err)

		err = consumeConn.Close()
		requireNoError(t, err)
	})

	// msgCounter is used to count the number of deliveries, to compare it afterwords.
	var msgCounter int

	handler := func(msg *clarimq.Delivery) clarimq.Action {
		requireEqual(t, message, string(msg.Body))

		msgCounter++

		doneChan <- struct{}{}

		return clarimq.Ack
	}

	queueName := stringGen()

	// creating a consumer.
	_, err := clarimq.NewConsumer(consumeConn, queueName, handler,
		clarimq.WithQueueOptionDurable(true),
		clarimq.WithConsumerOptionConsumerName(stringGen()),
	)
	requireNoError(t, err)

	// creating a publisher.
	publisher, err := clarimq.NewPublisher(publishConn)
	requireNoError(t, err)

	// publish a message.
	err = publisher.Publish(context.Background(), queueName, message)
	requireNoError(t, err)

	publishNotifyChan := publishConn.NotifyErrors()
	consumeNotifyChan := consumeConn.NotifyErrors()

	wg := &sync.WaitGroup{}

	wg.Add(4)

	// handling the failed recovery notification.
	go handleFailedRecovery(publishNotifyChan, wg)
	go handleFailedRecovery(consumeNotifyChan, wg)

	// waiting for the handler to process the delivery.
	<-doneChan

	// comparing the msgCounter that should be incremented by the consumer handler.
	requireEqual(t, 1, msgCounter)

	// shutting down the rabbitmq container to simulate a connection loss.
	err = exec.Command("docker", "compose", "stop", "rabbitmq").Run()
	requireNoError(t, err)

	// waiting for the failed recovery notification to finish handling.
	wg.Wait()

	// bringing the rabbitmq container up again.
	err = exec.Command("docker", "compose", "up", "-d").Run()
	requireNoError(t, err)

	// polling to check the container health.
	for range time.NewTicker(1 * time.Second).C {
		status, err := exec.Command("docker", "inspect", "-f", "{{.State.Health.Status}}", "rabbitmq").Output()
		requireNoError(t, err)

		if strings.ReplaceAll(string(status), "\n", "") == "healthy" {
			break
		}
	}

	// manually recovering.
	err = publishConn.Recover()
	requireNoError(t, err)

	err = consumeConn.Recover()
	requireNoError(t, err)

	// publish a new message to the queue after the recovery.
	err = publisher.Publish(context.Background(), queueName, message)
	requireNoError(t, err)

	// waiting for the recovered consumer to process the new message.
	<-doneChan

	// comparing the msgCounter again that should now be incremented by the consumer handler to 2.
	requireEqual(t, 2, msgCounter)
}

func handleFailedRecovery(chn <-chan error, wg *sync.WaitGroup) {
	for err := range chn {
		var recoveryErr *clarimq.RecoveryFailedError

		if errors.As(err, &recoveryErr) {
			wg.Done()
		}
	}
}

func Test_Recovery_PublishingCache(t *testing.T) { //nolint:paralleltest // intentional: must not run in parallel
	message := "test-message"

	publishConn := getConnection(t,
		clarimq.WithConnectionOptionBackOffFactor(1),
		clarimq.WithConnectionOptionRecoveryInterval(500*time.Millisecond),
	)

	consumeConn := getConnection(t,
		clarimq.WithConnectionOptionBackOffFactor(1),
		clarimq.WithConnectionOptionRecoveryInterval(500*time.Millisecond),
	)

	t.Cleanup(func() {
		err := publishConn.Close()
		requireNoError(t, err)

		err = consumeConn.Close()
		requireNoError(t, err)
	})

	wg := &sync.WaitGroup{}

	wg.Add(1)

	handler := func(msg *clarimq.Delivery) clarimq.Action {
		requireEqual(t, message, string(msg.Body))

		wg.Done()

		return clarimq.Ack
	}

	queueName := stringGen()

	// creating a consumer.
	_, err := clarimq.NewConsumer(consumeConn, queueName, handler,
		clarimq.WithQueueOptionDurable(true),
	)
	requireNoError(t, err)

	// creating a publisher.
	publisher, err := clarimq.NewPublisher(publishConn,
		clarimq.WithPublishOptionMandatory(true),
		clarimq.WithPublisherOptionPublishingCache(cache.NewBasicMemoryCache()),
	)
	requireNoError(t, err)

	t.Cleanup(func() {
		err := publisher.Close()
		requireNoError(t, err)
	})

	err = publisher.Publish(context.Background(), queueName, message)
	requireNoError(t, err)

	wg.Wait()

	// shutting down the rabbitmq container to simulate a connection loss.
	err = exec.Command("docker", "compose", "stop", "rabbitmq").Run()
	requireNoError(t, err)

	for i := 0; i < 4; i++ {
		// publish messages to the queue with while not connected.
		if err := publisher.Publish(context.Background(), queueName, message); !errors.Is(err, clarimq.ErrPublishFailedChannelClosedCached) {
			t.Fatal()
		}
	}

	// bringing the rabbitmq container up again.
	err = exec.Command("docker", "compose", "up", "-d").Run()
	requireNoError(t, err)

	wg.Add(4)

	// waiting for the recovered consumer to process the cached messages.
	wg.Wait()

	_, err = consumeConn.RemoveQueue(queueName, false, false, false)
	requireNoError(t, err)
}

// ##### helper functions: ##########################

// Returns a new connection with the given options.
func getConnection(t *testing.T, options ...clarimq.ConnectionOption) *clarimq.Connection {
	t.Helper()

	conn, err := clarimq.NewConnection(clarimq.SettingsToURI(&clarimq.ConnectionSettings{
		UserName: "guest",
		Password: "guest",
		Host:     "localhost",
		Port:     5672,
	}),
		options...,
	)

	requireNoError(t, err)

	return conn
}

// Compares two values and reports an error if they are not equal.
func requireEqual(t *testing.T, expected any, actual any) {
	t.Helper()

	equal := reflect.DeepEqual(expected, actual)

	if !equal {
		t.Errorf("Not equal: \nExpected: %v\nActual: %+v", expected, actual)
	}
}

// Ensures that err is nil, otherwise it reports an error.
func requireNoError(t *testing.T, err error) {
	t.Helper()

	if err != nil {
		t.Fatal(err)
	}
}

// Generates a random string for names (e.g. queue-names, exchange-names, routing-keys)
// that need to be unique since almost all tests run in parallel.
func stringGen() string {
	buffer := make([]byte, 16)

	_, err := rand.Read(buffer)
	if err != nil {
		return ""
	}

	return hex.EncodeToString(buffer)
}
