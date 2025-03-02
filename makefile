.PHONY: help clean test lint fix-lint uv-env dev-install build

# Default target
.DEFAULT_GOAL := help

help:
	@echo "Available make targets:"
	@echo "  help        - Show this help message"
	@echo "  clean       - Remove build artifacts and cache files"
	@echo "  test        - Run tests with pytest"
	@echo "  lint        - Run linting with ruff"
	@echo "  fix-lint    - Automatically fix linting issues"
	@echo "  uv-env      - Create a virtual environment with uv"
	@echo "  dev-install - Install package in development mode with dev dependencies"
	@echo "  build       - Build the package"

clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf .pytest_cache/
	rm -rf .ruff_cache/
	rm -rf .coverage
	find . -type d -name __pycache__ -exec rm -rf {} +

test:
	pytest --cov=polars_jdbc_tools tests/

lint:
	ruff check .
	
fix-lint:
	ruff check --fix .

uv-env:
	python -m pip install uv
	uv venv

dev-install:
	uv pip install -e ".[dev]"

# Main requested targets
build:
	python -m pip install build
	python -m build
	
# Combination targets
setup: uv-env dev-install

# Install in development mode
install-local:
	pip install -e .

all: clean lint test build