IMAGE ?= santadepapaya/thanos-promql-connector
TAG ?= 0.0.17-ggl10
PLATFORM ?= linux/amd64
DOCKER ?= docker
GO_TAGS ?= slicelabels

IMAGE_REF := $(IMAGE):$(TAG)

.PHONY: help image test build push build-push buildx-load

help:
	@echo "Targets:"
	@echo "  make test        Run Go tests with tags $(GO_TAGS)"
	@echo "  make build       Build $(IMAGE_REF) for $(PLATFORM)"
	@echo "  make push        Push $(IMAGE_REF)"
	@echo "  make build-push  Build and push $(IMAGE_REF) for $(PLATFORM)"
	@echo "  make buildx-load Build with buildx and load into local Docker"
	@echo ""
	@echo "Variables:"
	@echo "  IMAGE=$(IMAGE)"
	@echo "  TAG=$(TAG)"
	@echo "  PLATFORM=$(PLATFORM)"
	@echo "  GO_TAGS=$(GO_TAGS)"

image:
	@echo "$(IMAGE_REF)"

test:
	go test -tags=$(GO_TAGS) ./...

build:
	$(DOCKER) build --platform=$(PLATFORM) --build-arg GO_TAGS=$(GO_TAGS) -t $(IMAGE_REF) .

push:
	$(DOCKER) push $(IMAGE_REF)

build-push:
	$(DOCKER) buildx build --platform=$(PLATFORM) --build-arg GO_TAGS=$(GO_TAGS) -t $(IMAGE_REF) --push .

buildx-load:
	$(DOCKER) buildx build --platform=$(PLATFORM) --build-arg GO_TAGS=$(GO_TAGS) -t $(IMAGE_REF) --load .
