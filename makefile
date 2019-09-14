.PHONY: test
test:
	docker-compose -f test/docker-compose.yml down -v
	docker-compose -f test/docker-compose.yml up --build --exit-code-from test test

.PHONY: bench
bench:
	docker-compose -f test/docker-compose.yml down -v
	docker-compose -f test/docker-compose.yml up --build --exit-code-from bench bench

.PHONY: release-minor
release-minor:
	git diff --cached --exit-code > /dev/null &&\
	VERSION=$$(docker run --rm -v "$$PWD":/app -w /app node:11-alpine npm version minor) &&\
	git add package.json &&\
	git commit -m "release $$VERSION" &&\
	git tag -a "$$VERSION" -m "release $$VERSION"

.PHONY: release-patch
release-patch:
	git diff --cached --exit-code > /dev/null &&\
	VERSION=$$(docker run --rm -v "$$PWD":/app -w /app node:11-alpine npm version patch) &&\
	git add package.json &&\
	git commit -m "release $$VERSION" &&\
	git tag -a "$$VERSION" -m "release $$VERSION"

.PHONY: repl
repl:
	-docker-compose -f test/docker-compose.yml build \
		&& docker-compose -f test/docker-compose.yml run --rm repl
	docker-compose -f test/docker-compose.yml down -v

.PHONY: psql
psql:
	-docker-compose -f test/docker-compose.yml build postgres \
	  && docker-compose -f test/docker-compose.yml run --rm psql
	docker-compose -f test/docker-compose.yml down -v

.PHONY: lint
lint:
	docker-compose -f test/docker-compose.yml down -v
	docker-compose -f test/docker-compose.yml up --build --exit-code-from lint lint
