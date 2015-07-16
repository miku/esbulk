SHELL := /bin/bash
TARGETS = esbulk

# http://docs.travis-ci.com/user/languages/go/#Default-Test-Script
test:
	go get -d && go test -v

imports:
	goimports -w .

fmt:
	go fmt ./...

all: fmt test
	go build

install:
	go install

clean:
	go clean
	rm -f coverage.out
	rm -f $(TARGETS)
	rm -f esbulk-*.x86_64.rpm
	rm -f debian/esbulk_*.deb
	rm -f esbulk_*.deb
	rm -rf debian/esbulk/usr

cover:
	go get -d && go test -v	-coverprofile=coverage.out
	go tool cover -html=coverage.out

esbulk:
	go build cmd/esbulk/esbulk.go

# ==== packaging

deb: $(TARGETS)
	mkdir -p debian/esbulk/usr/sbin
	cp $(TARGETS) debian/esbulk/usr/sbin
	cd debian && fakeroot dpkg-deb --build esbulk .

REPOPATH = /usr/share/nginx/html/repo/CentOS/6/x86_64

publish: rpm
	cp esbulk-*.rpm $(REPOPATH)
	createrepo $(REPOPATH)

rpm: $(TARGETS)
	mkdir -p $(HOME)/rpmbuild/{BUILD,SOURCES,SPECS,RPMS}
	cp ./packaging/esbulk.spec $(HOME)/rpmbuild/SPECS
	cp $(TARGETS) $(HOME)/rpmbuild/BUILD
	./packaging/buildrpm.sh esbulk
	cp $(HOME)/rpmbuild/RPMS/x86_64/esbulk*.rpm .
