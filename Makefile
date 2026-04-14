DEVCONTAINER ?=

ifeq ($(DEVCONTAINER),true)
# Re-invoke any target inside the devcontainer
DEVCONTAINER_CLI := npx -y @devcontainers/cli

.PHONY: devcontainer-up
devcontainer-up:
	@$(DEVCONTAINER_CLI) up --workspace-folder .

Makefile: ;

%: devcontainer-up
	@$(DEVCONTAINER_CLI) exec --workspace-folder . make $@ $(filter-out DEVCONTAINER=true,$(MAKEOVERRIDES))

else

.PHONY: build clean test submodules run run-clean fmt critest e2e smoke patch patch-save reviewable

KUBE_VERSION := $(shell grep 'k8s.io/kubernetes v' go.mod | head -1 | awk '{print $$2}')
KUBE_MAJOR := $(word 1,$(subst ., ,$(KUBE_VERSION:v%=%)))
KUBE_MINOR := $(word 2,$(subst ., ,$(KUBE_VERSION:v%=%)))
VERSION_PKG := k8s.io/component-base/version
BUILD_DATE := $(shell date -u +'%Y-%m-%dT%H:%M:%SZ')

VERSION_LDFLAGS := \
	-X $(VERSION_PKG).gitVersion=$(KUBE_VERSION) \
	-X $(VERSION_PKG).gitMajor=$(KUBE_MAJOR) \
	-X $(VERSION_PKG).gitMinor=$(KUBE_MINOR) \
	-X $(VERSION_PKG).buildDate=$(BUILD_DATE)

patch:
	@cd kubernetes && git reset --hard HEAD
	@cd kubernetes && git apply ../patches/kubernetes.patch

patch-save:
	@cd kubernetes && git diff > ../patches/kubernetes.patch
	@echo "Patch saved to patches/kubernetes.patch"

ARTIFACT ?=

ifdef ARTIFACT
build:
	mv $(ARTIFACT) nanokube
	chmod +x nanokube
else
build: patch
	CGO_ENABLED=0 go build -ldflags="-s -w $(VERSION_LDFLAGS)" -o nanokube .
	@ls -lh nanokube | awk '{print "Binary size:", $$5}'
endif

clean:
	@rm -f nanokube; true
	@pkill -f '[.]\/nanokube' 2>/dev/null; true
	@docker ps -aq | xargs -r docker rm -f 2>/dev/null; true
	@docker volume ls -q | xargs -r docker volume rm -f 2>/dev/null; true
	@docker system prune -f >/dev/null 2>&1; true
	@rm -rf ~/.nanokube ~/.nanokube-e2e ~/.nanokube-critest ~/.nanokube-smoke ~/.nanokube-tmp.*
	@rm -rf ~/.kube

test:
	go test ./...

submodules:
	git submodule update --init --recursive --depth 1

fmt:
	go fmt ./...

ARGS ?=
VERBOSE_FLAGS := $(if $(filter 2,$(V)),-vv,$(if $(filter 1,$(V)),-v))

run: fmt build
	./nanokube $(VERBOSE_FLAGS) $(ARGS)

run-clean: ARGS += --clean
run-clean: run


WHAT ?=
CRITEST_SKIP ?= Mount Propagation|Mount Readonly|AppArmor
SUITE ?=

V ?= 0
NANOKUBE_OUT = $(if $(filter 1,$(V)),,>/dev/null 2>&1)

# Usage: $(call run-nanokube,<nanokube-args>,<ready-check>,<test-cmd>)
define run-nanokube
	@export TMPDIR=$$(mktemp -d "$$HOME/.nanokube-tmp.XXXXXX"); \
	trap 'kill $$! 2>/dev/null; wait' EXIT; \
	./nanokube $(1) $(ARGS) --name $(NAME) $(NANOKUBE_OUT) & \
	echo "########################################"; \
	echo "# NANOKUBE LOG: ~/.$(NAME)/log"; \
	echo "########################################"; \
	while ! $(2); do echo "waiting: $(2)"; sleep 1; done; \
	$(3)
endef

e2e: NAME = nanokube-e2e
e2e: build
	$(call run-nanokube,,\
		kubectl get nodes >/dev/null 2>&1,\
		cd tests && make test GROUP=e2e KIND=false $(if $(SUITE),SUITE=$(SUITE)) $(if $(WHAT),WHAT=$(WHAT)))

# TODO unhardcode docker
critest: NAME = nanokube-critest
critest: build
	$(call run-nanokube,--kubelet=false,\
		[ -S "$$HOME/.$(NAME)/docker/cri.sock" ],\
		cd cri-tools && go mod tidy && go test -c ./cmd/critest && ./critest.test --ginkgo.v $(if $(WHAT),--ginkgo.focus '$(WHAT)') $(if $(CRITEST_SKIP),--ginkgo.skip '$(CRITEST_SKIP)') --runtime-endpoint "unix://$$HOME/.$(NAME)/docker/cri.sock" --image-endpoint "unix://$$HOME/.$(NAME)/docker/cri.sock")

smoke: NAME = nanokube-smoke
smoke: build
	$(call run-nanokube,,\
		kubectl get nodes 2>&1,\
		cd tests && make test GROUP=smoke KIND=false $(if $(WHAT),WHAT=$(WHAT)))

reviewable: critest e2e

endif
