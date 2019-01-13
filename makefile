.PHONY: test
test:
	docker-compose -f test/docker-compose.yml down -v
	docker-compose -f test/docker-compose.yml up --build --exit-code-from test test

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
	-docker-compose -f test/docker-compose.yml run --rm repl
	docker-compose -f test/docker-compose.yml down -v

	# docker run -it --rm -v "$$PWD":/app -w /app node:11-alpine node bin/pgwire.js
