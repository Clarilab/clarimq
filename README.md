## INFO

This library is a wrapper around the [Go AMQP Client Library](https://github.com/rabbitmq/amqp091-go).

This library also includes support for:
- structured logging to multiple writers
- automatic recovery
- retry functionality

Supported Go Versions

This library supports the most recent Go, currently 1.21

## INSTALL

```bash
go get github.com/Clarilab/clarimq
```

## USAGE

### The Connection

First a connection instance needs to be initialized.
The connection can be configured by passing needed connection options.
Also there is the possibility to fully customize the configuration by passing a *ConnectionOptions* struct with the corresponding option.
To ensure correct escaping of the URI, the *SettingsToURI* function can be used to convert a *ConnectionSettings* struct to a valid URI.

#### Note:
*Although it is possible to publish and consume with one connection, it is best practice to use two separate connections for publisher and consumer activities.*

##### Connection with some options:
```Go
conn := clarimq.NewConnection("amqp://admin:password@localhost:5672/", 
	clarimq.WithConnectionOptionConnectionName(service-name),
	// more options can be passed
)
```

##### Connection with custom options:
```Go
connectionSettings := &clarimq.ConnectionSettings{
	UserName: "username",
	Password: "password",
	Host:     "host",
	Port:     5672,
}

connectionOptions := &clarimq.ConnectionOptions{
	Config: &clarimq.Config{
		ChannelMax:      0,
		FrameSize:       0,
		Heartbeat:       0,
		TLSClientConfig: &tls.Config{},
		Properties:      map[string]interface{}{},
		Locale:          "",
	},
	PrefetchCount:     1,
	ReconnectInterval: 1,
},

conn := clarimq.NewConnection(clarimq.SettingsToURI(connectionSettings), 
	clarimq.WithCustomConnectionOptions(connectionOptions),
)
```

When the connection is no longer needed, it should be closed to conserve resources.
```Go
conn.Close()
```

### Publish messages

To publish messages a publisher instance needs to be created. A previously created connection must be handed over to the publisher.

The publisher can be configured by passing needed connector options.
Also there is the possibility to fully customize the configuration by passing a *PublishOptions* struct with the corresponding option. 

```Go
publisher, err := clarimq.NewPublisher(conn,
	clarimq.WithPublishOptionAppID("my-application"),
	clarimq.WithPublishOptionExchange("my-exchange"),
	// more options can be passed
)
if err != nil {
	// handle error
}
```
The publisher can then be used to publish messages.
The target can be a queue name, or a topic if the publisher is configured to publish messages to an exchange.

Simple publish:
```Go
err = publisher.Publish(context.Background(), "my-target", "my-message")
if err != nil {
	// handle error
}
```

Optionally the *PublishWithOptions* method can be used to configure the publish options just for this specific publish.
The Method also gives the possibility to publish to multiple targets at once.

Publish with options:
```Go
err = publisher.PublishWithOptions(context.Background(), []string{"my-target-1","my-target-2"}, "my-message",
	clarimq.WithPublishOptionMessageID("99819a3a-388f-4199-b7e6-cc580d85a2e5"),
	clarimq.WithPublishOptionTracing("7634e958-1509-479e-9246-5b80ad8fc64c"),
)
if err != nil {
	// handle error
}
return nil
```

### Consume Messages

To consume messages a consumer instance needs to be created. A previously created connection must be handed over to the consumer.

The consumer can be configured by passing needed consume options.
Also there is the possibility to fully customize the configuration by passing a *ConsumeOptions* struct with the corresponding option. 

```Go
consumer, err = clarimq.NewConsumer(conn, "my-queue", handler(),
		clarimq.WithConsumerOptionConsumerName("my-consumer"),
	// more options can be passed
)
if err != nil {
	// handle error
}
```

The consumer can be used to declare exchanges, queues and queue-bindings:
```Go
consumer, err := clarimq.NewConsumer(conn, "my-queue", handler(),
		clarimq.WithConsumerOptionConsumerName("my-consumer"),
		clarimq.WithExchangeOptionDeclare(true),
		clarimq.WithExchangeOptionKind(clarimq.ExchangeTopic),
		clarimq.WithExchangeOptionName("my-exchange"),
		clarimq.WithQueueOptionDeclare(false), // is enabled by default, can be used to disable the default behavior
		clarimq.WithConsumerOptionBinding(
			clarimq.Binding{
				RoutingKey: "my-routing-key",
			},
		),
		// more options can be passed
	)
	if err != nil {
		// handle error
	}
```

The consumer can be closed to stop consuming if needed. The consumer does not need to be explicitly closed for a graceful shutdown if its connection is closed afterwards. However when using the retry functionality without providing a connection, the consumer must be closed for a graceful shutdown of the retry connection to conserve resources.
```Go
consumer.Close()
```

### Logging:
Structured logging is supported with the golang "log/slog" package. 
A text- or json-logger can be specified with the desired log level. 
The logs are written to a io.Writer that also can be specified.

Note: Multiple loggers can be specified!

```Go
jsonBuff := new(bytes.Buffer)
textBuff := new(bytes.Buffer)

conn := clarimq.NewConnection(connectionSettings, 
	clarimq.WithConnectionOptionTextLogging(os.Stdout, slog.LevelInfo),
	clarimq.WithConnectionOptionTextLogging(textBuff, slog.LevelWarn),
	clarimq.WithConnectionOptionJSONLogging(jsonBuff, slog.LevelDebug),
)
```

### Return Handler:
When publishing mandatory messages, they will be returned if it is not possible to route the message to the given destination. A return handler can be specified to handle the the return. The return contains the original message together with some information such as an error code and an error code description.

If no return handler is specified a log will be written to the logger at warn level.

```Go
returnHandler := func(r clarimq.Return) {
	// handle the return
}

conn := clarimq.NewConnection(connectionSettings, 
	clarimq.WithConnectionOptionReturnHandler(
		clarimq.ReturnHandler(returnHandler),
	),
)
```

### Recovery:

This library provides an automatic recovery with build-in exponential back-off functionality. When the connection to the server is lost, the recovery will automatically try to reconnect. You can adjust the parameters of the back-off algorithm:

```Go
conn, err = clarimq.NewConnection(settings,
	clarimq.WithConnectionOptionReconnectInterval(2),    // default is 1 second
	clarimq.WithConnectionOptionBackOffFactor(3),        // default is 2
	clarimq.WithConnectionOptionMaxReconnectRetries(16), // default is 10
)
```

For the case the maximum number of retries is reached, the connection provides a *NotifyAutoRecoveryFail* method which provides a channel that will return an error for you to handle:

```Go
handleFailedRecovery := func(failedRecovery <-chan error) {
	for err := range failedRecovery {
		if err != nil {
			// handle failed recovery
		}
	}
}

conn, err = clarimq.NewConnection(settings)

handleFailedRecovery(conn.NotifyAutoRecoveryFail())
```

### Retry:

This library includes a retry functionality with a dead letter exchange and dead letter queues. To use the retry, some parameters have to be set:

```Go
connectionSettings := &clarimq.ConnectionSettings{
	UserName: "username",
	Password: "password",
	Host:     "host",
	Port:     5672,
}

publishConn, err := clarimq.NewConnection(clarimq.SettingsToURI(connectionSettings))
if err != nil {
	// handle error
}

consumeConn, err := clarimq.NewConnection(clarimq.SettingsToURI(connectionSettings))
if err != nil {
	// handle error
}

retryOptions := &clarimq.RetryOptions{
	RetryConn: publishConn,
	Delays: []time.Duration{
		time.Second,
		time.Second * 2,
		time.Second * 3,
		time.Second * 4,
		time.Second * 5,
	},
	MaxRetries: 5,
	Cleanup:    true, // only set this to true if you want to remove all retry related queues and exchanges when closing the consumer
},

consumer, err := clarimq.NewConsumer(consumeConn, queueName, handler,
		clarimq.WithConsumerOptionDeadLetterRetry(retryOptions),
	)
```

It is recommended to provide a separate publish connection for the retry functionality. If no connection is specified, a separate connection is established internally. 

For each given delay a separate dead letter queue is declared. When a delivery is nacked by the consumer, it is republished via the delay queues one after another until it is acknowledged or the specified maximum number of retry attempts is reached. 

## External packages

[Go AMQP Client Library](https://github.com/rabbitmq/amqp091-go)