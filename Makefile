BINARY ?= bin/pompos
UV ?= uv
VENV ?= .venv
LOCAL_PYTHON := $(abspath $(VENV)/bin/python)
LOCAL_INGESTR := $(abspath $(VENV)/bin/ingestr)

.PHONY: setup build run test vet docker-build docker-up docker-down

setup: $(VENV)/.pompos-deps

$(VENV)/.pompos-deps: requirements-local.txt
	$(UV) venv --allow-existing --no-project $(VENV)
	$(UV) pip install --python $(VENV)/bin/python -r requirements-local.txt
	touch $@

build:
	mkdir -p $(dir $(BINARY))
	go build -o $(BINARY) ./cmd/pompos

run: setup
	POMPOS_PYTHON_BINARY="$(LOCAL_PYTHON)" POMPOS_INGESTR_BINARY="$(LOCAL_INGESTR)" go run ./cmd/pompos

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
