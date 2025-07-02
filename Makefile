.DEFAULT_GOAL := help

.PHONY: help
help: ## Display this help screen
	@echo "Available targets:"
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z0-9_\/-]+:.*##/ {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: tidy
tidy: ## Tidy go.mod file
	go mod tidy -v
	go mod verify

.PHONY: lint
lint: ## Run linter
	golangci-lint run -v -c .golangci.yaml

.PHONY: test
test: ## Run tests
	go test -race -count=1 ./...

.PHONY: test/cover
test/cover: ## Run tests with coverage
	go test -race -count=1 ./... -coverprofile=cover.out ./...
	go tool cover -html=cover.out
