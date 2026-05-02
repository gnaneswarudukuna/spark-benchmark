# Makefile for spark-benchmark
# Author: Gnaneswarudu Kuna — COMPSCI 532

.PHONY: help setup parse benchmark test charts clean

help:
	@echo "Available commands:"
	@echo "  make setup      - Install dependencies"
	@echo "  make parse      - Run log parsing pipeline"
	@echo "  make benchmark  - Run full benchmark"
	@echo "  make test       - Run all tests"
	@echo "  make charts     - Generate comparison charts"
	@echo "  make clean      - Remove generated files"

setup:
	pip install -r requirements.txt

parse:
	python3 -m src.parsing.log_parser

perhost-rdd:
	python3 -m src.queries.perhost_profiling.rdd

perhost-dataframe:
	python3 -m src.queries.perhost_profiling.dataframe

perhost-sql:
	python3 -m src.queries.perhost_profiling.sql

sessionization-rdd:
	python3 -m src.queries.sessionization.rdd

sessionization-dataframe:
	python3 -m src.queries.sessionization.dataframe

sessionization-sql:
	python3 -m src.queries.sessionization.sql

benchmark:
	python3 -m benchmark.run_benchmark

test:
	pytest tests/ -v

charts:
	python3 results/generate_charts.py

analysis:
	python3 analysis/results_analysis.py

clean:
	rm -rf data/processed/
	rm -rf results/charts/
	rm -rf results/wall_clock.json
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -name "*.pyc" -delete
