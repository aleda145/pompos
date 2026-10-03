FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/pompos ./cmd/pompos

FROM python:3.12-slim-bookworm
COPY --from=ghcr.io/astral-sh/uv:0.8.13 /uv /usr/local/bin/uv
RUN useradd --create-home --uid 10001 pompos \
    && mkdir -p /data/ingestions \
    && chown -R pompos:pompos /data
COPY --from=build /out/pompos /usr/local/bin/pompos
USER pompos
WORKDIR /app
ENV POMPOS_ADDRESS=:8080 \
    POMPOS_DATA_DIR=/data \
    POMPOS_DESTINATION_PATH=/data/pompos.duckdb \
    POMPOS_METADATA_PATH=/data/pompos.sqlite
EXPOSE 8080
VOLUME ["/data"]
ENTRYPOINT ["pompos"]
