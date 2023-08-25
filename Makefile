all: vet lint vuln test_all

vet:
	go vet ./...

lint:
	golangci-lint run ./...

vuln:
	govulncheck ./...

test_all: test test_integration

test:
	go test -skip "(Test_Integration|Test_Reconnection)" -vet=off -failfast -race -coverprofile=coverage.out

test_integration:
	./run_integration_tests.sh Test_Integration

test_reconnection:
	./run_integration_tests.sh Test_Reconnection

test_all: test test_integration test_reconnection