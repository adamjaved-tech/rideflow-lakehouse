# Developer commands (Linux/macOS). Windows users: see README for the PowerShell equivalents.
PY ?= python
export PYTHONPATH := src

.PHONY: setup test test-unittest validate generate run health queries demo docker-build clean

setup:
	$(PY) -m pip install -r requirements-dev.txt

test:
	$(PY) -m pytest -q

test-unittest:  # no pytest needed (tests are unittest-compatible)
	$(PY) -m unittest discover -s tests -v

validate:
	$(PY) scripts/validate_repo.py

generate:
	$(PY) -m rideflow generate --out data/sample/events.jsonl

run:
	$(PY) -m rideflow run

health:
	$(PY) -m rideflow health

queries:
	@for q in $$($(PY) -m rideflow query); do echo "== $$q"; $(PY) -m rideflow query $$q; done

demo: run health queries

docker-build:
	docker build -t rideflow-lakehouse .

clean:
	rm -rf data/lake data/warehouse reports run.log .pytest_cache
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
