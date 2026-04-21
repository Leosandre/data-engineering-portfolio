.PHONY: help setup generate produce deploy batch metrics extract dashboard teardown verify-cleanup

PROFILE ?= agenda-facil
REGION  ?= us-east-1

help: ## Lista todos os targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

setup: ## Cria venv e instala dependencias
	python3 -m venv .venv
	.venv/bin/pip install -e ".[dev]" --quiet

generate: ## Gera transacoes sinteticas (local JSONL)
	.venv/bin/python src/generate_transactions.py

deploy: ## Provisiona infra AWS (CloudFormation)
	bash scripts/deploy.sh $(PROFILE) $(REGION)

produce: ## Envia transacoes pro Kinesis + popula DynamoDB (30min, 2000 TPS)
	.venv/bin/python src/producer.py --profile $(PROFILE) --region $(REGION) --duration 1800 --tps 2000

seed: ## So popula DynamoDB (sem enviar pro Kinesis)
	.venv/bin/python src/producer.py --profile $(PROFILE) --region $(REGION) --seed-only

batch: ## Roda deteccao batch (local, mesmas regras)
	.venv/bin/python scripts/batch_detection.py --local

metrics: ## Coleta metricas de latencia
	.venv/bin/python scripts/collect_metrics.py --profile $(PROFILE) --region $(REGION)

extract: ## Extrai evidencias (S3, DynamoDB, SQS, CloudWatch) pra local — RODAR ANTES DO TEARDOWN
	.venv/bin/python scripts/extract_evidence.py --profile $(PROFILE) --region $(REGION)

dashboard: ## Gera dashboard HTML (usa dados locais de data/evidence/)
	.venv/bin/python scripts/generate_dashboard.py

teardown: ## Destroi toda a infra AWS
	bash scripts/teardown.sh $(PROFILE) $(REGION)

verify-cleanup: ## Verifica que nenhum recurso sobrou
	bash scripts/verify_cleanup.sh $(PROFILE) $(REGION)

# --- Fluxo completo ---
# 1. make setup
# 2. make generate          (gera dados locais pra teste)
# 3. make deploy            (sobe infra AWS)
# 4. make produce           (envia pro Kinesis por 2h — Flink ja esta processando)
# 5. make batch             (roda deteccao batch pra comparacao)
# 6. make extract           (CRITICO: baixa tudo pra local ANTES de destruir)
# 7. make dashboard         (gera dashboard offline com evidencias locais)
# 8. make teardown          (destroi tudo)
# 9. make verify-cleanup    (confirma que nao sobrou nada)
