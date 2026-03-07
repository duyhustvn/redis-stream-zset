IMG=redis-stream-zset
IMG_TAG=1.0.0

docker-build:
	docker build -t ${IMG}:${IMG_TAG} .

performance-test:
	k6 run performance_test.js

