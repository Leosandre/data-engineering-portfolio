.PHONY: ingest transform validate dashboard all clean

all: ingest transform validate dashboard

ingest:
	python src/ingest.py

transform:
	python src/transform.py

validate:
	python src/validate.py

dashboard:
	python src/dashboard.py

clean:
	rm -rf data/bronze/*.parquet data/silver/*.parquet data/gold/*.parquet docs/dashboard.html
