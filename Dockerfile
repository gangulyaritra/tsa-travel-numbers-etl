FROM ghcr.io/astral-sh/uv:debian

LABEL maintainer="ganguly.aritra@outlook.com"

RUN apt update && apt upgrade -y

WORKDIR /app

# Copy Build Files.
COPY pyproject.toml pyproject.toml
COPY uv.lock uv.lock
COPY src src

# Environment Variables.
ENV PYTHONFAULTHANDLER=1 \
PYTHONUNBUFFERED=1 \
UV_NO_SYNC=1

RUN uv sync

ENTRYPOINT ["/bin/bash"]