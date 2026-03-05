docker-build:
	docker build -t redis-stream:1.0.0 .

performance-test:
	k6 run performance_test.js

