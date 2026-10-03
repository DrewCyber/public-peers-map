# ymonitor collector — single static binary in a distroless image.
# Build (from the repo root):  docker build -f deploy/Dockerfile -t ymonitor .
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ymonitor ./cmd/ymonitor \
    && mkdir -p /data && chown 65532:65532 /data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ymonitor /ymonitor
COPY --from=build --chown=65532:65532 /data /data
VOLUME /data
# Health endpoint should be reachable through port mappings inside containers.
ENV DATA_DIR=/data HEALTH_ADDR=0.0.0.0:8080
EXPOSE 8080
USER nonroot
ENTRYPOINT ["/ymonitor"]
