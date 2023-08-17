all: vet lint vuln test

vet:
	go vet ./...

lint:
	golangci-lint run ./...

vuln:
	govulncheck ./...

test_all: test test_integration

test:
	go test -skip "Test_Integration" -vet=off -failfast -race -coverprofile=coverage.out

test_integration:
	./run_integration_tests.sh