# syntax=docker/dockerfile:1
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X github.com/tabreu8/mqtt-get/internal/core.Version=${VERSION}" \
    -o /out/mqtt-get ./cmd/mqtt-get

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/mqtt-get /mqtt-get
ENV HTTP_ADDR=:8080 DATA_DIR=/data
VOLUME /data
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s CMD ["/mqtt-get", "healthcheck"]
ENTRYPOINT ["/mqtt-get"]
