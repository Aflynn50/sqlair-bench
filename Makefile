build:
	go build -o sqlair-bench ./...
	docker compose build

run: build
	docker compose up
