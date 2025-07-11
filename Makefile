.DEFAULT_GOAL := all

.PHONY: all
all: ## Show the available make targets.
	@echo "Usage: make <target>"
	@echo ""
	@echo "Targets:"
	@fgrep "##" Makefile | fgrep -v fgrep

.PHONY: clean
clean: ## Clean the temporary files.
	rm -rf .pytest_cache
	rm -rf .mypy_cache
	rm -rf .coverage
	rm -rf .ruff_cache
	rm -rf megalinter-reports

.PHONY: format
format:  ## Format the code.
	pipenv run black .
	pipenv run ruff check . --fix

.PHONY: lint
lint:  ## Run all linters (black/ruff/pylint/mypy).
	pipenv run black --check .
	pipenv run ruff check .
	make mypy

.PHONY: test
test:  integration-down integration-up ## Run the tests and check coverage.
	ENVIRONMENT=INTEGRATION_TESTS pipenv run pytest tests/ --cov=srm_autoprocessor --cov-report term-missing --cov-fail-under=90
	docker compose -f tests/integration/docker-compose.yml down

.PHONY: unit-test
unit-test:  ## Run just the unit tests and check coverage.
	ENVIRONMENT=TEST pipenv run pytest tests/unit --cov=srm_autoprocessor --cov-report term-missing --cov-fail-under=90

.PHONY: integration-up
integration-up:  ## Bring up the Docker Compose to services the integration tests depend on (database etc.)
	docker compose -f tests/integration/docker-compose.yml up -d
	bash ./tests/integration/wait_for_dependencies.sh

.PHONY: integration-down
integration-down:  ## Tear down the integration test docker containers
	docker compose -f tests/integration/docker-compose.yml down

.PHONY: integration-tests
integration-tests: integration-down integration-up  ## Run the integration tests (and the services they depend on)
	ENVIRONMENT=INTEGRATION_TESTS pipenv run pytest tests/integration
	docker compose -f tests/integration/docker-compose.yml down

.PHONY: mypy
mypy:  ## Run mypy.
	pipenv run mypy srm_autoprocessor

.PHONY: install
install:  ## Install the dependencies excluding dev.
	pipenv install

.PHONY: install-dev
install-dev:  ## Install the dependencies including dev.
	pipenv install --dev

.PHONY: megalint
megalint:  ## Run the mega-linter.
	docker run --platform linux/amd64 --rm \
		-v /var/run/docker.sock:/var/run/docker.sock:rw \
		-v $(shell pwd):/tmp/lint:rw \
		oxsecurity/megalinter:v8

.PHONY: docker-build
docker-build:  ## Build the Docker image
	docker build -t europe-west2-docker.pkg.dev/ssdc-rm-ci/docker/srm-autoprocessor .

build: clean install-dev format lint test docker-build  ## Run a fresh install, check, test and docker build
