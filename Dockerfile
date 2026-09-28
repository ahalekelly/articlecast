FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev
COPY app.py feeds.txt ./
CMD ["uv", "run", "--no-sync", "app.py"]
