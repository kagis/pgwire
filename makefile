.PHONY: test
test:
	docker-compose -f test/docker-compose.yml down -v
	docker-compose -f test/docker-compose.yml up --build --exit-code-from test
