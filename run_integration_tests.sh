#!/bin/sh

# The container engine can be set with CONTAINER_CLI, e.g. CONTAINER_CLI=podman (default: docker).
# The container settings mirror docker-compose.yaml.
CLI=${CONTAINER_CLI:-docker}
CONTAINER=rabbitmq
IMAGE=docker.io/library/rabbitmq:3.13.3-alpine

$CLI rm -f -v $CONTAINER > /dev/null 2>&1

# runs as rabbitmq user, so the healthcheck CLI can not create the erlang cookie as root,
# which the broker then fails to read (podman runs the first healthcheck right at start).
$CLI run -d \
    --name $CONTAINER \
    --user rabbitmq \
    -p 5672:5672 \
    --health-cmd "rabbitmq-diagnostics -q ping" \
    --health-interval 5s \
    --health-timeout 10s \
    --health-retries 5 \
    $IMAGE || exit 1

echo "Checking service availability for $CONTAINER (CTRL+C to exit)"

while [ "$($CLI inspect -f '{{.State.Health.Status}}' $CONTAINER)" != "healthy" ]; do
    sleep 1;
done

echo "Service $CONTAINER is now available"

CONTAINER_CLI=$CLI go test -run "$1" -vet=off -failfast -race -v -coverprofile=coverage.out
go_test_exit_code=$?

$CLI rm -f -v $CONTAINER > /dev/null

exit $go_test_exit_code
