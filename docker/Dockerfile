FROM golang:1.23-alpine AS builder
ARG SERVICE=api
WORKDIR /build
COPY src/go.mod ./
RUN go mod download
COPY src/ ./
RUN go mod tidy && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags='-s -w' -o /bin/service ./cmd/${SERVICE}

FROM alpine:3.20
RUN addgroup -S app && adduser -S -G app app && apk add --no-cache ca-certificates
COPY --from=builder /bin/service /usr/local/bin/service
USER app
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/service"]
