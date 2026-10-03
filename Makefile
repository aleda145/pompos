BINARY ?= bin/pompos
UV ?= uv
VENV ?= .venv

.PHONY: setup setup-test-python build run test vet docker-build docker-up docker-down

setup:
	$(UV) --version
	$(UV) python find python3

setup-test-python: $(VENV)/.pompos-deps

$(VENV)/.pompos-deps: requirements-local.txt internal/runner/python/requirements.txt
	$(UV) venv --allow-existing --no-project $(VENV)
	$(UV) pip install --python $(VENV)/bin/python -r requirements-local.txt
	touch $@

build:
	mkdir -p $(dir $(BINARY))
	go build -o $(BINARY) ./cmd/pompos

run: setup
	POMPOS_UV_BINARY="$(UV)" go run ./cmd/pompos

test:
	go test ./...

vet:
	go vet ./...

docker-build:
	docker compose build

docker-up:
	docker compose up --build

docker-down:
	docker compose down
