FROM golang:1.22-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/pompos ./cmd/pompos

FROM python:3.12-slim-bookworm
ARG INGESTR_VERSION=1.1.8
ARG DUCKDB_VERSION=1.5.3
RUN pip install --no-cache-dir "ingestr==${INGESTR_VERSION}" "duckdb==${DUCKDB_VERSION}" \
    && useradd --create-home --uid 10001 pompos \
    && mkdir -p /data/ingestions \
    && chown -R pompos:pompos /data
COPY --from=build /out/pompos /usr/local/bin/pompos
USER pompos
WORKDIR /app
ENV POMPOS_DATA_DIR=/data \
    POMPOS_DESTINATION_PATH=/data/pompos.duckdb \
    POMPOS_METADATA_PATH=/data/pompos.sqlite \
    POMPOS_INGESTR_BINARY=/usr/local/bin/ingestr
EXPOSE 8080
VOLUME ["/data"]
ENTRYPOINT ["pompos"]
