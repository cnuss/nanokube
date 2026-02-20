.PHONY: build clean test submodules run

build:
	@cd kubernetes && git apply ../patches/kubernetes/001-scheduler-use-cmd-context.patch 2>/dev/null || true
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
