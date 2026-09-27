FROM golang:1.22-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/pompos ./cmd/pompos

FROM python:3.12-slim-bookworm
COPY requirements-local.txt /tmp/requirements-local.txt
RUN pip install --no-cache-dir -r /tmp/requirements-local.txt \
    && useradd --create-home --uid 10001 pompos \
    && mkdir -p /data/ingestions \
    && chown -R pompos:pompos /data
COPY --from=build /out/pompos /usr/local/bin/pompos
USER pompos
WORKDIR /app
ENV POMPOS_DATA_DIR=/data \
    POMPOS_DESTINATION_PATH=/data/pompos.duckdb \
    POMPOS_METADATA_PATH=/data/pompos.sqlite
EXPOSE 8080
VOLUME ["/data"]
ENTRYPOINT ["pompos"]
