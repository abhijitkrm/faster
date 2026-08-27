FROM golang:1.23-alpine AS build

WORKDIR /app

ENV GOTOOLCHAIN=auto

RUN apk add --no-cache git

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o fasterindexer .

FROM alpine:3.19

RUN apk add --no-cache ca-certificates curl

WORKDIR /app

COPY --from=build /app/fasterindexer ./

EXPOSE 3102

HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
  CMD curl -f http://localhost:3102/health || exit 1

CMD ["./fasterindexer"]
