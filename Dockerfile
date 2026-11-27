FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG BIN=lode-adit
RUN CGO_ENABLED=0 go build -ldflags '-s -w' -o /out/app ./cmd/${BIN}

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/app /app
USER nonroot
ENTRYPOINT ["/app"]
