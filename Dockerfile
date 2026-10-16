# Build context: the services/ directory (needed for replace to ../cbl-kit and ../cbl-protos).
# Build: docker build -f cbl-ledger/Dockerfile -t cbl/ledger:dev .
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY cbl-kit/ /src/cbl-kit/
COPY cbl-protos/ /src/cbl-protos/
COPY cbl-ledger/ /src/cbl-ledger/
WORKDIR /src/cbl-ledger
RUN go mod download
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/ledger ./cmd/ledger

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /out/ledger /app/ledger
COPY --from=build /src/cbl-ledger/migrations /app/migrations
EXPOSE 8040 9040 9240
ENTRYPOINT ["/app/ledger"]
