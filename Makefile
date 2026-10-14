.PHONY: build test test-integration test-e2e smoke vet lint fmt vuln up down bench

build:
	go build ./...

test:
	go test ./...

test-integration:
	go test -tags integration -count=1 ./internal/...

# test-e2e запускает сквозной сценарий против уже поднятого compose-стенда (CBL-2.4).
test-e2e:
	go test -tags e2e -count=1 -v ./test/e2e/

# smoke поднимает compose-стенд, проверяет миграции и гоняет e2e-сценарий резервов.
smoke:
	powershell -NoProfile -File test/e2e/reserve_flow.ps1

# bench запускает все бенчмарки записи леджера на testcontainers PostgreSQL (CBL-2.9).
bench:
	go test -tags integration -run "^$$" -bench "." -benchtime=3s ./internal/storage/

vet:
	go vet ./...

# lint монтирует каталог services/ целиком: replace на ../cbl-kit резолвится в контейнере.
lint:
	docker run --rm -v "$(CURDIR)/..":/src -w /src/$(notdir $(CURDIR)) golangci/golangci-lint:latest-alpine golangci-lint run ./...

fmt:
	gofmt -l .

vuln:
	go run golang.org/x/vuln/cmd/govulncheck@latest ./...

up:
	docker compose -f deploy/docker-compose.yml --profile app up -d

down:
	docker compose -f deploy/docker-compose.yml down -v
