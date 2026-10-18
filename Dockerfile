# syntax=docker/dockerfile:1
# Multi-stage build of the streamq broker and CLI (M8). The runtime image is distroless and runs
# as the nonroot user (uid 65532); /var/lib/streamq is created owned by that user so named volumes
# mounted there inherit the ownership.

FROM golang:1.26.0-alpine AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOFLAGS=-trimpath
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -ldflags="-s -w" -o /out/streamq-broker ./cmd/streamq-broker && \
    go build -ldflags="-s -w" -o /out/streamqctl ./cmd/streamqctl && \
    mkdir -p /out/data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/streamq-broker /out/streamqctl /usr/local/bin/
COPY --from=build --chown=65532:65532 /out/data /var/lib/streamq
USER nonroot:nonroot
EXPOSE 9092 9093 9094 9100
ENTRYPOINT ["/usr/local/bin/streamq-broker"]
CMD ["--config", "/etc/streamq/broker.yaml"]
