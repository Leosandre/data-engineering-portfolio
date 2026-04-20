# ============================================================
# POC S3 Tables — Automação
# ============================================================
# Uso: make <target>
# ============================================================

REGION       := us-east-1
STACK_PREFIX := s3tables-poc
PROFILE      := default
DATA_DIR     := ./data
METRICS_DIR  := ./metrics

.PHONY: download deploy upload etl schema-evolution time-travel queries maintenance dashboard teardown clean

# ----------------------------------------------------------
# 1. Download do dataset NYC Taxi
# ----------------------------------------------------------
download:
	@echo "📥 Baixando dataset NYC Taxi..."
	bash scripts/01_download_dataset.sh

# ----------------------------------------------------------
# 2. Deploy da infraestrutura (CloudFormation)
# ----------------------------------------------------------
deploy:
	@echo "🏗️  Deployando infraestrutura..."
	bash scripts/02_deploy_infra.sh $(REGION) $(STACK_PREFIX) $(PROFILE)

# ----------------------------------------------------------
# 3. Upload do dataset para S3
# ----------------------------------------------------------
upload:
	@echo "☁️  Fazendo upload do dataset..."
	bash scripts/03_upload_dataset.sh $(REGION) $(STACK_PREFIX) $(PROFILE)

# ----------------------------------------------------------
# 4. Executar ETL (batches em cada pipeline)
# ----------------------------------------------------------
etl:
	@echo "⚙️  Executando Glue Jobs..."
	bash scripts/04_run_etl.sh $(REGION) $(STACK_PREFIX) $(PROFILE)

# ----------------------------------------------------------
# 5. Schema Evolution
# ----------------------------------------------------------
schema-evolution:
	@echo "🔄 Executando schema evolution..."
	python3 scripts/05_schema_evolution.py --region $(REGION) --profile $(PROFILE)

# ----------------------------------------------------------
# 6. Time Travel
# ----------------------------------------------------------
time-travel:
	@echo "⏰ Executando time travel..."
	python3 scripts/06_time_travel.py --region $(REGION) --profile $(PROFILE)

# ----------------------------------------------------------
# 7. Queries comparativas
# ----------------------------------------------------------
queries:
	@echo "🔍 Executando queries comparativas..."
	python3 scripts/07_run_queries.py --region $(REGION) --profile $(PROFILE)

# ----------------------------------------------------------
# 8. Métricas de manutenção
# ----------------------------------------------------------
maintenance:
	@echo "🔧 Coletando métricas de manutenção..."
	python3 scripts/08_collect_maintenance.py --region $(REGION) --profile $(PROFILE)

# ----------------------------------------------------------
# 9. Dashboard
# ----------------------------------------------------------
dashboard:
	@echo "📊 Gerando dashboard..."
	python3 scripts/09_generate_dashboard.py

# ----------------------------------------------------------
# Cleanup
# ----------------------------------------------------------
teardown:
	@echo "🗑️  Destruindo infraestrutura..."
	bash scripts/99_teardown.sh $(REGION) $(STACK_PREFIX) $(PROFILE)

clean:
	rm -rf $(DATA_DIR) $(METRICS_DIR)/*.json
