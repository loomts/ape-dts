# IMG ?= apecloud/ape-dts
BUILD_IMG ?= apecloud/ape-dts-env
RELASE_IMG ?= apecloud/ape-dts
VERSION ?= 0.1
CONFIG_PATH ?= ./example/mysql_snapshot_sample.yaml

# make release-build GIT_TOKEN=xxx
.PHONY: release-build
release-build:
	cd images && ./build.sh "$(BUILD_IMG):$(VERSION)" "$(GIT_TOKEN)"
# make docker-build CONFIG_PATH=xxx
.PHONY: docker-build
docker-build:
	cd images && \
	docker build -t $(RELASE_IMG):$(VERSION) --build-arg LOCAL_CONFIG_PATH=$(CONFIG_PATH) -f Dockerfile_release . 


