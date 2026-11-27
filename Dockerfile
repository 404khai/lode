FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG BIN=lode-adit
RUN CGO_ENABLED=0 go build -ldflags '-s -w' -o /out/app ./cmd/${BIN} && mkdir -p /out/data

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/app /app
COPY --from=build --chown=65532:65532 /out/data /data
USER nonroot
ENTRYPOINT ["/app"]
