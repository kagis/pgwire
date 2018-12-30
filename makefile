.PHONY: test
test:
	docker-compose -f test/docker-compose.yml down -v
	docker-compose -f test/docker-compose.yml up --build --exit-code-from test

.PHONY: release-minor
release-minor:
	git diff --cached --exit-code > /dev/null &&\
	VERSION=$$(docker run --rm -v "$$PWD":/app -w /app node:11-alpine npm version minor) &&\
	git add package.json &&\
	git commit -m "release $$VERSION" &&\
	git tag -a "$$VERSION" -m "release $$VERSION"

release-patch:
