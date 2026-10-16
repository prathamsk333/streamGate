.PHONY: setup fmt lint typecheck test itest check kafka-up kafka-down docker example bench clean

setup:
	uv sync

fmt:
	uv run ruff format .
	uv run ruff check --fix .

lint:
	uv run ruff check .
	uv run ruff format --check .

typecheck:
	uv run mypy

test:
	uv run pytest -m "not integration" --cov --cov-report=term

itest:
	uv run pytest -m integration

check: lint typecheck test

kafka-up:
	docker compose -f docker/compose.ci.yml up -d --wait

kafka-down:
	docker compose -f docker/compose.ci.yml down -v

docker:
	docker build -f docker/Dockerfile -t streamgate:dev .

example:
	docker compose -f docker/compose.example.yml up -d --build

bench:
	uv run python scripts/bench.py local

clean:
	rm -rf dist .pytest_cache .mypy_cache .ruff_cache .coverage htmlcov
