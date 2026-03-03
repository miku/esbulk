PKGNAME := esbulk
TARGETS := esbulk
VERSION := 0.7.32
SHELL := /bin/bash

# testing against elasticsearch may require larger amounts of memory
.PHONY: test
test:
	go test -cover -v

.PHONY: imports
imports:
	goimports -w .

.PHONY: fmt
fmt:
	go fmt ./...

.PHONY: all
all: fmt test
	go build -o esbulk cmd/esbulk/main.go

.PHONY: install
install:
	go install

.PHONY: clean
clean:
	go clean
	rm -f coverage.out
	rm -f $(TARGETS)
	rm -f esbulk-*.x86_64.rpm
	rm -f esbulk-* .aarch64.rpm
	rm -f esbulk_*.deb
	rm -rf logs/

.PHONY: cover
cover:
	go get -d && go test -v	-coverprofile=coverage.out
	go tool cover -html=coverage.out

esbulk:
	CGO_ENABLED=0 go build -o esbulk cmd/esbulk/main.go

# ==== packaging ====
#
# Packaging deb, rpm requires "nfpm" https://nfpm.goreleaser.com/
#
# $ go install github.com/goreleaser/nfpm/v2/cmd/nfpm@latest

.PHONY: image
image:
	DOCKER_CONTENT_TRUST=0 docker build --rm -t tirtir/esbulk:latest -t tirtir/esbulk:$(VERSION) .
	docker image prune --force --filter label=stage=intermediate

.PHONY: rmi
rmi:
	docker rmi tirtir/esbulk:$(VERSION)

.PHONY: deb
deb: $(TARGETS)
	GOARCH=amd64 SEMVER=$(VERSION) nfpm package -p deb
	GOARCH=arm64 SEMVER=$(VERSION) nfpm package -p deb

.PHONY: rpm
rpm: $(TARGETS)
	GOARCH=amd64 SEMVER=$(VERSION) nfpm package -p rpm
	GOARCH=arm64 SEMVER=$(VERSION) nfpm package -p rpm

