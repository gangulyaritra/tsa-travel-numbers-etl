.PHONY: init build run format format.check lint.check type.check

init:
    uv --version || curl -LsSf https://astral.sh/uv/install.sh | sh

build:
    docker build -t tsa-checkpoint-etl:latest .

run:
    docker run -it --rm --env-file .env --cpus="0.25" --memory="256m" --net=host tsa-checkpoint-etl:latest

format:
    uv run ruff format .

format.check:
    uv run ruff format --check .

lint.check:
    uv run ruff check .

type.check:
    uv run mypy src/tsa_checkpoint

test:
    uv run pytest