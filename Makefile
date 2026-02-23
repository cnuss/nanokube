.PHONY: build clean test submodules run fmt patch-save critest

patch-kubernetes:
	@cd kubernetes && git reset --hard
	@cd kubernetes && git apply ../patches/kubernetes.patch

patch-save:
	@cd kubernetes && git diff > ../patches/kubernetes.patch
	@echo "Patch saved to patches/kubernetes.patch"

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

fmt:
	gofmt -w .

run: fmt build
	./nanokube --clean

CRITEST_DIR := /tmp/nanokube-critest
CRITEST_SOCK := $(CRITEST_DIR)/cri.sock

critest: build
	@bash -c '\
		cleanup() { \
			echo "Cleaning up..."; \
			[ -n "$$NANOKUBE_PID" ] && kill $$NANOKUBE_PID 2>/dev/null && wait $$NANOKUBE_PID 2>/dev/null; \
			rm -rf $(CRITEST_DIR); \
		}; \
		trap cleanup EXIT INT TERM HUP; \
		rm -rf $(CRITEST_DIR); \
		echo "Starting nanokube for critest..."; \
		./nanokube --clean --data $(CRITEST_DIR) & \
		NANOKUBE_PID=$$!; \
		echo "Waiting for CRI socket (pid $$NANOKUBE_PID)..."; \
		for i in $$(seq 1 30); do \
			[ -S $(CRITEST_SOCK) ] && break; \
			sleep 1; \
		done; \
		if [ ! -S $(CRITEST_SOCK) ]; then \
			echo "CRI socket not found after 30s"; \
			exit 1; \
		fi; \
		echo "Running critest..."; \
		critest --runtime-endpoint unix://$(CRITEST_SOCK) --image-endpoint unix://$(CRITEST_SOCK); \
	'
