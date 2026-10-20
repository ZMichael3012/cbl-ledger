.PHONY: build test test-integration test-e2e bench vet lint fmt vuln up down

build:
	go build ./...

test:
	go test ./...

test-integration:
	go test -tags integration -count=1 ./internal/...

# test-e2e runs the Go e2e tests against an already started compose stand
# (CBL-2.4/2.5/2.7/2.10); the run instruction is test/e2e/E2E.md.
test-e2e:
	go test -tags e2e -count=1 -v ./test/e2e/

# E2E: a run is performed by an agent following the test/e2e/E2E.md instruction
# (platform-independent; artifacts go to test/e2e/.runs/<UTC mark>/); there are
# no PowerShell run scripts.

# bench runs the ledger write benchmarks on testcontainers PostgreSQL (CBL-2.9).
bench:
	go test -tags integration -run "^$$" -bench "." -benchtime=3s ./internal/storage/

vet:
	go vet ./...

# lint mounts the repository; the modules come from the GitHub origin (Go proxy)
# resolves inside the container.
lint:
	docker run --rm -v "$(CURDIR)":/src -w /src golangci/golangci-lint:latest-alpine golangci-lint run ./...

fmt:
	gofmt -l .

vuln:
	go run golang.org/x/vuln/cmd/govulncheck@latest ./...

up:
	docker compose -f deploy/docker-compose.yml --profile app up -d

down:
	docker compose -f deploy/docker-compose.yml down -v
