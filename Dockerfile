# syntax=docker/dockerfile:1
FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath \
    -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT} -X main.buildDate=${BUILD_DATE}" \
    -o /pgsc-mirror ./cmd/pgsc-mirror

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /pgsc-mirror /usr/local/bin/pgsc-mirror
ENTRYPOINT ["/usr/local/bin/pgsc-mirror"]

