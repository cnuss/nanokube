.PHONY: build clean test submodules run run-clean fmt patch-save critest init e2e helm

CRITEST_VERSION := v1.35.0
KUTTL_VERSION := 0.25.0

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
	go fmt ./...

ARGS ?=

run: fmt build
	./nanokube $(ARGS)

run-clean: ARGS += --clean
run-clean: run

KUTTL_ARCH = $(shell go env GOARCH | sed 's/amd64/x86_64/')

init:
	@which critest >/dev/null 2>&1 || curl -fsSL https://github.com/kubernetes-sigs/cri-tools/releases/download/$(CRITEST_VERSION)/critest-$(CRITEST_VERSION)-$$(go env GOOS)-$$(go env GOARCH).tar.gz | tar xz -C $$(go env GOPATH)/bin critest
	@which kubectl-kuttl >/dev/null 2>&1 || curl -fsSLo $$(go env GOPATH)/bin/kubectl-kuttl https://github.com/kudobuilder/kuttl/releases/download/v$(KUTTL_VERSION)/kubectl-kuttl_$(KUTTL_VERSION)_$$(go env GOOS)_$(KUTTL_ARCH) && chmod +x $$(go env GOPATH)/bin/kubectl-kuttl

WHAT ?=

DATA ?= /tmp/nanokube-test

# Usage: $(call run-nanokube,<nanokube-args>,<ready-check>,<test-cmd>)
define run-nanokube
	@mkdir -p $(DATA); \
	trap 'kill $$! 2>/dev/null; wait' EXIT; \
	./nanokube $(1) $(ARGS) --clean --data $(DATA) & \
	for i in $$(seq 1 30); do $(2) && break; sleep 1; done; \
	echo "########################################"; \
	echo "# NANOKUBE LOG: $(DATA)/log"; \
	echo "########################################"; \
	$(3)
endef

e2e: build
	$(call run-nanokube,,\
		kubectl get nodes >/dev/null 2>&1,\
		kubectl kuttl test --config tests/kuttl-test.yaml $(if $(WHAT),--test $(WHAT)))

helm: build
	$(call run-nanokube,,\
		kubectl get nodes >/dev/null 2>&1,\
		helm upgrade --install guestbook tests/helm/guestbook -n guestbook --create-namespace --wait --timeout 120s)

# TODO unhardcode docker
critest: build
	$(call run-nanokube,--kubelet=false,\
		[ -S "$(DATA)/docker/cri.sock" ],\
		critest --ginkgo.v --ginkgo.focus 'Conformance' $(if $(WHAT),--ginkgo.focus '$(WHAT)') --runtime-endpoint "unix://$(DATA)/docker/cri.sock" --image-endpoint "unix://$(DATA)/docker/cri.sock")
