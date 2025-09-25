.PHONY: format lint check-format install-dev

install-dev:
	pip install -e ".[dev,test]"

format:
	black egon_validation/ examples/ tests/

check-format:
	black --check egon_validation/ examples/ tests/

lint:
	flake8 egon_validation/ examples/ tests/

check: check-format lint
	@echo "All checks passed!"

fix: format
	@echo "Code formatted with black"