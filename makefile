.PHONY: test
test:
	docker-compose -f test/docker-compose.yml down -v
	docker-compose -f test/docker-compose.yml up --build --exit-code-from test

.PHONY: pub
pub:
	docker run -i --rm -v "$$PWD":/app -w /app node:11-alpine npm pub