FROM golang:1.25-alpine AS builder

WORKDIR /

COPY go.mod go.sum ./
RUN go mod download

COPY main.go .

# Build the Go binary for Linux
# CGO_ENABLED=0 is important for static binaries that work with scratch/alpine images
RUN CGO_ENABLED=0 go build -o app

FROM alpine

WORKDIR /

COPY --from=builder /app /app

CMD ["/app"]
