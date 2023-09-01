#!/bin/sh

docker-compose down
docker-compose up -d


CONTAINER=rabbitmq

echo "Checking service availability for $CONTAINER (CTRL+C to exit)"

while [ "$(docker inspect -f {{.State.Health.Status}} $CONTAINER)" != "healthy" ]; do
    sleep 1;
done

echo "Service $CONTAINER is now available"

go test -run "$1" -vet=off -failfast -race -coverprofile=coverage.out
go_test_exit_code=$?

docker-compose down

exit $go_test_exit_code