# Contributing

Use Go 1.25 or newer, run `gofmt`, and keep production builds CGo-free. New reconciliation behavior should be exercised with a tiny synthetic `httptest` upstream; automated tests must not crawl or bulk-download the public PGS Catalog FTP site.

Before opening a change:

```bash
CGO_ENABLED=0 go test ./...
go test -race ./...
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o pgsc-mirror.linux ./cmd/pgsc-mirror
```

Tests using a real GCS bucket must remain opt-in and isolate all objects beneath a unique disposable prefix.

