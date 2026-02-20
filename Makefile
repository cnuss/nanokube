.PHONY: build clean test submodules run

patch-kubernetes:
	@cd kubernetes && git reset --hard
	@cd kubernetes && git apply ../patches/kubernetes.patch

patch: patch-kubernetes

build: patch
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
