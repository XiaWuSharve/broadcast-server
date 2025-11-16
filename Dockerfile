FROM golang:1.25.4-alpine3.21 AS builder

ENV CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64

WORKDIR /build

COPY . .

RUN go mod download

RUN mkdir -p /app

RUN go build -o /app/app .
COPY ./home.html /app/

FROM alpine:3.21 AS final

COPY --from=builder /app/* /app/
WORKDIR /app

EXPOSE 8080

CMD [ "./app" ]