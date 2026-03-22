.PHONY: build clean test submodules run run-clean fmt patch-save critest init e2e
CRITEST := cri-tools/critest

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
	@rm -f nanokube
	@pkill -f nanokube 2>/dev/null; true
	@docker ps -aq | xargs -r docker rm -f 2>/dev/null; true
	@docker volume ls -q | xargs -r docker volume rm -f 2>/dev/null; true
	@docker system prune -f >/dev/null 2>&1; true
	@rm -rf ~/.nanokube ~/.nanokube-e2e ~/.nanokube-critest

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

$(CRITEST):
	cd cri-tools && go test -c -o critest ./cmd/critest/

init:
	cd tests && make install-chainsaw

WHAT ?=
SUITE ?=

V ?= 0
NANOKUBE_OUT = $(if $(filter 1,$(V)),,>/dev/null 2>&1)

# Usage: $(call run-nanokube,<nanokube-args>,<ready-check>,<test-cmd>)
define run-nanokube
	@trap 'kill $$! 2>/dev/null; wait' EXIT; \
	./nanokube $(1) $(ARGS) --name $(NAME) $(NANOKUBE_OUT) & \
	for i in $$(seq 1 30); do $(2) && break; sleep 1; done; \
	echo "########################################"; \
	echo "# NANOKUBE LOG: ~/.$(NAME)/log"; \
	echo "########################################"; \
	$(3)
endef

e2e: NAME = nanokube-e2e
e2e: build
	$(call run-nanokube,,\
		kubectl get nodes >/dev/null 2>&1,\
		cd tests && make test KIND=false $(if $(SUITE),SUITE=$(SUITE)) $(if $(WHAT),WHAT=$(WHAT)))

# TODO unhardcode docker
critest: NAME = nanokube-critest
critest: build $(CRITEST)
	$(call run-nanokube,--kubelet=false,\
		[ -S "$$HOME/.$(NAME)/docker/cri.sock" ],\
		$(CRITEST) --ginkgo.v $(if $(WHAT),--ginkgo.focus '$(WHAT)') --runtime-endpoint "unix://$$HOME/.$(NAME)/docker/cri.sock" --image-endpoint "unix://$$HOME/.$(NAME)/docker/cri.sock")

