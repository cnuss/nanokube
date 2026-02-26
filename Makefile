.PHONY: build clean test unit-test submodules run fmt patch-save critest init

CRITEST_VERSION := v1.35.0

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

unit-test:
	go test ./...

test: build
	@trap 'kill $$PID 2>/dev/null; wait 2>/dev/null' EXIT; \
	./nanokube --clean & PID=$$!; \
	export KUBECONFIG=$$HOME/.nanokube/kubeconfig; \
	mkdir -p $$HOME/.nanokube/volumes/test-pvc; \
	for i in $$(seq 1 30); do kubectl get nodes >/dev/null 2>&1 && break; sleep 1; done; \
	sed "s|NANOKUBE_DATADIR|$$HOME/.nanokube|g" tests/pods/all-volumes.yaml | kubectl apply -f -; \
	for i in $$(seq 1 30); do [ "$$(kubectl get pod all-volumes -o jsonpath='{.status.phase}' 2>/dev/null)" = "Running" ] && break; sleep 1; done; \
	CID=$$(docker ps --filter "label=io.kubernetes.pod.name=all-volumes" --filter "label=io.kubernetes.container.name=busybox" --format '{{.ID}}'); \
	docker logs "$$CID"; \
	echo ""; echo "Press Ctrl+C to stop"; \
	wait

submodules:
	git submodule update --init --recursive

fmt:
	go fmt ./...

ARGS ?= --clean -vv

run: fmt build
	./nanokube $(ARGS)

init:
	go install github.com/kubernetes-sigs/cri-tools/cmd/critest@$(CRITEST_VERSION)

critest: build
	@D=$$(mktemp -d); \
	trap 'kill $$! 2>/dev/null; wait 2>/dev/null; rm -rf "$$D"' EXIT; \
	./nanokube --kubelet=false --data "$$D" & \
	for i in $$(seq 1 30); do [ -S "$$D/cri.sock" ] && break; sleep 1; done; \
	critest --runtime-endpoint "unix://$$D/cri.sock" --image-endpoint "unix://$$D/cri.sock"
