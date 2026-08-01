BINARY := pgsc-mirror
VERSION ?= dev
COMMIT ?= unknown
BUILD_DATE ?= unknown
LDFLAGS := -s -w -X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.buildDate=$(BUILD_DATE)

.PHONY: linux test race clean

linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags "$(LDFLAGS)" -o $(BINARY).linux ./cmd/pgsc-mirror

test:
	CGO_ENABLED=0 go test ./...

race:
	go test -race ./...

clean:
	$(RM) $(BINARY) $(BINARY).linux

