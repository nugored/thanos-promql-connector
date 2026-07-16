FROM golang:1.23-alpine3.22 AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY *.go ./

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/thanos-promql-connector

FROM alpine:3.22

WORKDIR /app

RUN apk add --no-cache ca-certificates \
    && adduser -D -H -u 65532 appuser

COPY --from=build /out/thanos-promql-connector ./thanos-promql-connector

USER appuser

ENTRYPOINT ["./thanos-promql-connector"]
