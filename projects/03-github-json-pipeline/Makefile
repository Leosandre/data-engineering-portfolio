.PHONY: ingest flatten analyze dashboard all clean

all: ingest flatten analyze dashboard

ingest:
	python src/ingest.py

flatten:
	python src/flatten.py

analyze:
	python src/analyze.py

dashboard:
	python src/dashboard.py

clean:
	rm -rf data/raw/*.jsonl data/normalized/*.parquet data/analytics/*.parquet docs/dashboard.html
