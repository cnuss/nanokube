.PHONY: build clean test submodules

build:
	CGO_ENABLED=0 go build -ldflags="-s -w" -o nanokube .

clean:
	rm -f nanokube

test:
	go test ./...

submodules:
	git submodule update --init --recursive
