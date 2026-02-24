.PHONY: build clean test submodules run fmt patch-save critest

VERSION_PKG := k8s.io/component-base/version
KUBE_GIT_VERSION := $(shell cd kubernetes && git describe --tags --match='v*' 2>/dev/null | sed 's/-g/-/')
KUBE_GIT_COMMIT := $(shell cd kubernetes && git rev-parse HEAD)
KUBE_GIT_TREE_STATE := $(shell cd kubernetes && if git diff --quiet 2>/dev/null; then echo clean; else echo dirty; fi)
KUBE_GIT_MAJOR := $(shell echo "$(KUBE_GIT_VERSION)" | sed 's/^v//' | cut -d. -f1)
KUBE_GIT_MINOR := $(shell echo "$(KUBE_GIT_VERSION)" | sed 's/^v//' | cut -d. -f2)
BUILD_DATE := $(shell date -u +'%Y-%m-%dT%H:%M:%SZ')

VERSION_LDFLAGS := \
	-X $(VERSION_PKG).gitVersion=$(KUBE_GIT_VERSION) \
	-X $(VERSION_PKG).gitCommit=$(KUBE_GIT_COMMIT) \
	-X $(VERSION_PKG).gitTreeState=$(KUBE_GIT_TREE_STATE) \
	-X $(VERSION_PKG).gitMajor=$(KUBE_GIT_MAJOR) \
	-X $(VERSION_PKG).gitMinor=$(KUBE_GIT_MINOR) \
	-X $(VERSION_PKG).buildDate=$(BUILD_DATE)

patch-kubernetes:
	@cd kubernetes && git reset --hard
	@cd kubernetes && git apply ../patches/kubernetes.patch

patch-save:
	@cd kubernetes && git diff > ../patches/kubernetes.patch
	@echo "Patch saved to patches/kubernetes.patch"

patch: patch-kubernetes

build: patch
	CGO_ENABLED=0 go build -ldflags="-s -w $(VERSION_LDFLAGS)" -o nanokube .
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
		./nanokube --clean --kubelet=false --data $(CRITEST_DIR) & \
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
