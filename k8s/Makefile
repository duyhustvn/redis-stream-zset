service=redis-stream-zset
apply:
	kubectl apply -f .

restart:
	kubectl rollout restart deploy $(service)

undeploy:
	kubectl uninstall $(service)
