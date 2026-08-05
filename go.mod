module github.com/Clarilab/clarimq/v2

go 1.25.0

require (
	github.com/davecgh/go-spew v1.1.1
	github.com/rabbitmq/amqp091-go v1.10.0
	go.opentelemetry.io/otel v1.44.0
	go.opentelemetry.io/otel/trace v1.44.0
)

replace github.com/rabbitmq/amqp091-go => github.com/dkPranav/amqp091-go v0.0.0-20250709082216-a7e551553dff

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel/metric v1.44.0 // indirect
)
