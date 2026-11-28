# Lantern: the web UI is built with Node, the server with Go.
FROM node:24-alpine AS ui
WORKDIR /ui
COPY web/lantern/package.json web/lantern/package-lock.json ./
RUN npm ci --no-fund --no-audit
COPY web/lantern/ ./
RUN npm run build

FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags '-s -w' -o /out/lode-lantern ./cmd/lode-lantern

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/lode-lantern /lode-lantern
COPY --from=ui /ui/dist /ui
USER nonroot
ENTRYPOINT ["/lode-lantern", "-ui", "/ui"]
