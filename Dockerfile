FROM golang:1.22-bookworm AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/atlas-worker ./cmd/worker
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/atlas-server ./cmd/server

FROM debian:bookworm-slim

WORKDIR /workspace

COPY --from=build /out/atlas-worker /usr/local/bin/atlas-worker
COPY --from=build /out/atlas-worker /workspace/worker
COPY --from=build /out/atlas-server /workspace/server

CMD ["atlas-worker"]
