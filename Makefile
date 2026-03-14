# Load .env if present (optional)
-include .env
export

BINARY     := server
BUILD_PATH := ./cmd/server
MIGRATE    := migrate -path migrations -database "$(DATABASE_URL)"

.PHONY: run run-local stop build test test-short test-integration migrate migrate-down lint lint-fix generate mocks clean help

help:
	@echo "Targets:"
	@echo "  run             - Start all services (app + postgres + redis) with docker compose"
	@echo "  run-local       - Start only postgres + redis, then run the server with go run"
	@echo "  stop            - Stop docker compose services"
	@echo "  build           - Build the server binary"
	@echo "  test            - Run full test suite (unit + integration; needs Docker)"
	@echo "  test-short      - Run unit tests only (skips Redis/Testcontainers)"
	@echo "  test-integration - Run full tests with Ryuk disabled (use if reaper fails)"
	@echo "  migrate         - Run DB migrations up (requires migrate CLI and DATABASE_URL)"
	@echo "  migrate-down    - Roll back last migration"
	@echo "  lint            - Run golangci-lint"
	@echo "  lint-fix        - Run golangci-lint with --fix (auto-fix safe issues)"
	@echo "  generate        - Generate mocks and Swagger docs"
	@echo "  swagger         - Regenerate Swagger docs only (docs/ and GET /swagger/index.html)"
	@echo "  mocks           - Generate mocks only"
	@echo "  clean           - Stop containers, remove volumes and binary"

run:
	docker compose up --build

run-local:
	docker compose up -d postgres redis
	@echo "Waiting for postgres and redis..."
	@sleep 3
	@export DATABASE_URL=$${DATABASE_URL:-postgres://postgres:secret@localhost:5432/notifications?sslmode=disable}; \
	export REDIS_URL=$${REDIS_URL:-localhost:6379}; \
	export PORT=$${PORT:-8080}; \
	go run $(BUILD_PATH)

stop:
	docker compose down

build:
	go build -o $(BINARY) $(BUILD_PATH)

test:
	go test -v -race -count=1 -timeout=120s ./...

test-short:
	go test -short -race -count=1 ./...

test-integration:
	TESTCONTAINERS_RYUK_DISABLED=true go test -v -race -count=1 -timeout=120s ./...

migrate:
	$(MIGRATE) up

migrate-down:
	$(MIGRATE) down 1

lint:
	golangci-lint run ./...

lint-fix:
	golangci-lint run --fix ./...

generate: mocks swagger

swagger:
	@go run github.com/swaggo/swag/cmd/swag@latest init -g cmd/server/main.go -o docs --parseDependency --parseInternal

mocks:
	go run github.com/vektra/mockery/v2@v2.52.2 --all --recursive --dir ./internal/service --output ./internal/mocks --case underscore
	go run github.com/vektra/mockery/v2@v2.52.2 --all --recursive --dir ./internal/repository --output ./internal/mocks --case underscore
	go run github.com/vektra/mockery/v2@v2.52.2 --all --recursive --dir ./internal/ratelimit --output ./internal/mocks --case underscore
	go run github.com/vektra/mockery/v2@v2.52.2 --all --recursive --dir ./internal/provider --output ./internal/mocks --case underscore

clean:
	docker compose down -v
	rm -f $(BINARY)