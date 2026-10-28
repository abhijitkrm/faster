FROM golang:1.23-alpine AS build

WORKDIR /app

ENV GOTOOLCHAIN=auto

RUN apk add --no-cache git

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o clobarchiver .

FROM alpine:3.19

RUN apk add --no-cache ca-certificates curl

WORKDIR /app

COPY --from=build /app/clobarchiver ./

EXPOSE 9102

HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
  CMD curl -f http://localhost:9102/health || exit 1

CMD ["./clobarchiver"]
