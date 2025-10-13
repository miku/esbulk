PKGNAME := esbulk
TARGETS := esbulk
VERSION := 0.7.26

# testing against elasticsearch may require larger amounts of memory
test:
	go test -cover -v

imports:
	goimports -w .

fmt:
	go fmt ./...

all: fmt test
	go build -o esbulk cmd/esbulk/main.go

install:
	go install

clean:
	go clean
	rm -f coverage.out
	rm -f $(TARGETS)
	rm -f esbulk-*.x86_64.rpm
	rm -f packaging/debian/esbulk_*.deb
	rm -f esbulk_*.deb
	rm -rf packaging/debian/esbulk/usr
	rm -rf logs/

cover:
	go get -d && go test -v	-coverprofile=coverage.out
	go tool cover -html=coverage.out

esbulk:
	CGO_ENABLED=0 go build -o esbulk cmd/esbulk/main.go

# ==== packaging

image:
	DOCKER_CONTENT_TRUST=0 docker build --rm -t tirtir/esbulk:latest -t tirtir/esbulk:$(VERSION) .
	docker image prune --force --filter label=stage=intermediate

rmi:
	docker rmi tirtir/esbulk:$(VERSION)

deb: $(TARGETS)
	mkdir -p packaging/debian/esbulk/usr/sbin
	cp $(TARGETS) packaging/debian/esbulk/usr/sbin
	mkdir -p packaging/debian/esbulk/usr/local/share/man/man1
	cp docs/esbulk.1 packaging/debian/esbulk/usr/local/share/man/man1
	cd packaging/debian && fakeroot dpkg-deb -Zzstd --build esbulk .
	mv packaging/debian/esbulk*deb .

rpm: $(TARGETS)
	# on deb based distros, you may need:
	# sudo rpm --initdb && sudo chmod -R a+r /var/lib/rpm/
	mkdir -p $(HOME)/rpmbuild/{BUILD,SOURCES,SPECS,RPMS}
	mkdir -p $(HOME)/rpmbuild/SOURCES/$(PKGNAME)
	cp ./packaging/rpm/$(PKGNAME).spec $(HOME)/rpmbuild/SPECS
	cp $(TARGETS) $(HOME)/rpmbuild/SOURCES/$(PKGNAME)
	cp docs/$(PKGNAME).1 $(HOME)/rpmbuild/SOURCES/$(PKGNAME)
	./packaging/rpm/buildrpm.sh $(PKGNAME)
	cp $(HOME)/rpmbuild/RPMS/x86_64/$(PKGNAME)-$(VERSION)*.rpm .


