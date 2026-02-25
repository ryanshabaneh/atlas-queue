FROM golang:1.22-bookworm AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/atlas-worker ./cmd/worker

FROM debian:bookworm-slim

WORKDIR /workspace

COPY --from=build /out/atlas-worker /usr/local/bin/atlas-worker
COPY --from=build /out/atlas-worker /workspace/worker

CMD ["atlas-worker"]
