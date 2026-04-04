.PHONY: ingest transform test docs dashboard all clean

all: ingest transform test dashboard

ingest:
	python src/ingest.py

transform:
	dbt run --profiles-dir .

test:
	dbt test --profiles-dir .

docs:
	dbt docs generate --profiles-dir .
	dbt docs serve --profiles-dir .

dashboard:
	python src/dashboard.py

clean:
	dbt clean --profiles-dir .
	rm -rf data/raw/*.parquet data/datasus_leitos.duckdb docs/dashboard.html
