all: vet lint vuln test_all

vet:
	go vet ./...

lint:
	golangci-lint run ./...

vuln:
	govulncheck ./...

test:
	go test -skip "(Test_Integration|Test_Recovery)" -vet=off -failfast -race -coverprofile=coverage.out

test_integration:
	./run_integration_tests.sh Test_Integration

test_recovery:
	./run_integration_tests.sh Test_Recovery

test_all: test test_integration test_recovery