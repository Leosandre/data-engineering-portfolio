.PHONY: help setup deploy upload-dags backfill evidence teardown verify-cleanup

STACK_PREFIX ?= b3-pipeline
REGION ?= us-east-1
PROFILE ?= agenda-facil

help:
	@echo "make setup          - cria virtualenv e instala dependencias"
	@echo "make deploy         - sobe stacks CloudFormation"
	@echo "make upload-dags    - envia DAGs, dbt e src pro S3 do MWAA"
	@echo "make backfill       - executa backfill de dezembro/2024"
	@echo "make evidence       - extrai screenshots e metricas"
	@echo "make teardown       - destroi todos os recursos AWS"
	@echo "make verify-cleanup - confirma que nao sobrou nada"

setup:
	python3 -m venv .venv
	.venv/bin/pip install -r requirements.txt

deploy:
	bash scripts/deploy.sh $(STACK_PREFIX) $(REGION) $(PROFILE)

upload-dags:
	bash scripts/upload_dags.sh $(STACK_PREFIX) $(REGION) $(PROFILE)

backfill:
	bash scripts/backfill.sh $(STACK_PREFIX) $(REGION) $(PROFILE)

evidence:
	bash scripts/extract_evidence.sh $(STACK_PREFIX) $(REGION) $(PROFILE)

teardown:
	bash scripts/teardown.sh $(STACK_PREFIX) $(REGION) $(PROFILE)

verify-cleanup:
	bash scripts/verify_cleanup.sh $(STACK_PREFIX) $(REGION) $(PROFILE)
