up:
	docker compose up --build

down:
	docker compose down

logs:
	docker compose logs -f

go:
	docker compose run --rm go $(filter-out $@,$(MAKECMDGOALS))

tidy:
	docker compose run --rm --profile tools go go mod tidy

scale-notification:
	docker compose up --scale worker-notification=$(n) --no-recreate
