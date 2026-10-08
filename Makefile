.PHONY: up down reset logs ps deps test fmt build bench smoke scale
up:
	docker compose up --build -d --scale worker=3

down:
	docker compose down

reset:
	docker compose down -v --remove-orphans

logs:
	docker compose logs -f api dispatcher worker

ps:
	docker compose ps

deps:
	cd src && go mod tidy

test: deps
	cd src && go test ./...

fmt:
	cd src && gofmt -w $$(find . -name '*.go')

build: deps
	cd src && go build ./...

bench: deps
	cd src && go run ./cmd/bench -n 10000 -concurrency 64 -timeout 5m

smoke:
	bash scripts/smoke.sh

scale:
	docker compose up -d --scale worker=5
