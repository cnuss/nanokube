.PHONY: build clean test submodules run fmt patch patch-save

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

CHAINSAW := go tool -modfile=tools/go.mod chainsaw

KUBE_PATCH := patches/kubernetes-$(KUBE_MAJOR).$(KUBE_MINOR).patch

patch:
	@cd kubernetes && git reset --hard HEAD
	@cd kubernetes && git apply ../$(KUBE_PATCH)

init:
	@git submodule update --init --recursive --depth 1
	@$(MAKE) patch

patch-save:
	@cd kubernetes && git diff > ../$(KUBE_PATCH)
	@echo "Patch saved to $(KUBE_PATCH)"

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
	@rm -rf ~/.nanokube ~/.nanokube-tmp.*
	@rm -rf ~/.kube

fmt:
	@go fmt ./...

SUITE ?=
CHAINSAW_ARGS ?=

test:
	$(CHAINSAW) test $(if $(SUITE),tests/$(SUITE)) $(CHAINSAW_ARGS)

ARGS ?=

run: fmt build
	./nanokube $(ARGS)

start: fmt build
	./nanokube start $(ARGS)

stop: fmt build
	./nanokube stop $(ARGS)
