IMG=redis-stream-zset
IMG_TAG=1.0.0

CONTAINER_REGISTRY = docker.io
USER = duyle95

docker-build:
	docker build -t ${IMG}:${IMG_TAG} .

docker-save:
	docker save -o ${IMG}.tar ${IMG}:${IMG_TAG}

docker-push:
	docker tag $(IMG):$(IMG_TAG) $(USER)/$(IMG):$(IMG_TAG)
	docker push $(CONTAINER_REGISTRY)/$(USER)/$(IMG):$(IMG_TAG)

performance-test:
	k6 run performance_test.js
