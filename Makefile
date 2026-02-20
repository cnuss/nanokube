.PHONY: build clean test submodules run

build:
	CGO_ENABLED=0 go build -ldflags="-s -w" -o nanokube .
	@ls -lh nanokube | awk '{print "Binary size:", $$5}'

clean:
	rm -f nanokube

test:
	go test ./...

submodules:
	git submodule update --init --recursive

run: build
	./nanokube --clean
