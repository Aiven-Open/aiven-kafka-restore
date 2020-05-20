PYTHON ?= PYTHONDONTWRITEBYTECODE=1 python3
PYTHON_SOURCE_DIRS = kafka_restore

.PHONY: all
all:

.PHONY: pylint
pylint:
	$(PYTHON) -m pylint --rcfile .pylintrc $(PYTHON_SOURCE_DIRS)

.PHONY: flake8
flake8:
	$(PYTHON) -m flake8 kafka_restore $(PYTHON_SOURCE_DIRS)
