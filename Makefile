IMAGE ?= santadepapaya/thanos-promql-connector
TAG ?= 0.0.5
PLATFORM ?= linux/amd64
DOCKER ?= docker

IMAGE_REF := $(IMAGE):$(TAG)

.PHONY: help image build push build-push buildx-load

help:
	@echo "Targets:"
	@echo "  make build       Build $(IMAGE_REF) for $(PLATFORM)"
	@echo "  make push        Push $(IMAGE_REF)"
	@echo "  make build-push  Build and push $(IMAGE_REF) for $(PLATFORM)"
	@echo "  make buildx-load Build with buildx and load into local Docker"
	@echo ""
	@echo "Variables:"
	@echo "  IMAGE=$(IMAGE)"
	@echo "  TAG=$(TAG)"
	@echo "  PLATFORM=$(PLATFORM)"

image:
	@echo "$(IMAGE_REF)"

build:
	$(DOCKER) build --platform=$(PLATFORM) -t $(IMAGE_REF) .

push:
	$(DOCKER) push $(IMAGE_REF)

build-push:
	$(DOCKER) buildx build --platform=$(PLATFORM) -t $(IMAGE_REF) --push .

buildx-load:
	$(DOCKER) buildx build --platform=$(PLATFORM) -t $(IMAGE_REF) --load .
