.PHONY: generate upload bronze silver gold ml dashboard all clean

all: generate

generate:
	python src/generate_data.py

reconcile:
	python src/reconcile.py

clean:
	rm -rf data/synthetic/*.parquet data/synthetic/*.json data/synthetic/*.csv
