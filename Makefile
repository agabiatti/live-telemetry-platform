# Orquestração leve: make + containers one-shot. Sem Airflow/Dagster (alvo de produção).
.DEFAULT_GOAL := help

GEN_DIR     ?= ../desafio-data-engineer
N_SESSIONS  ?= 2000

# Smoke e2e: lake isolado em data/smoke (não toca o lake demo), replay acelerado.
SMOKE_DIR   ?= data/smoke
SMOKE_N     ?= 300
SMOKE_ENV   = -e DATA_DIR=$(SMOKE_DIR) -e BRONZE_PATH=$(SMOKE_DIR)/bronze \
              -e SILVER_PATH=$(SMOKE_DIR)/silver -e GOLD_PATH=$(SMOKE_DIR)/gold

.PHONY: help generate up demo producer bronze gold compact test smoke down clean logs

help: ## Lista os targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

generate: ## Roda o gerador (em container) → ./data/raw  [N_SESSIONS=2000]
	@mkdir -p data/raw
	docker run --rm \
	  -v $(abspath $(GEN_DIR)):/gen \
	  -v $(abspath data/raw):/out \
	  -w /gen python:3.11-slim \
	  sh -c "pip install -q pyyaml pyarrow && python generate_all.py --n-sessions $(N_SESSIONS) --output-dir /out"

up: ## Sobe infra + serviços always-on (redpanda, console, silver, streamlit)
	docker compose up -d --build redpanda console silver streamlit

producer: ## Publica o dataset no broker (replay)
	docker compose --profile pipeline run --rm producer

bronze: ## Roda o job Bronze (drena tópico → Delta)
	docker compose --profile batch run --rm bronze

gold: ## Roda o batch Gold (dbt → Parquet)
	docker compose --profile batch run --rm gold

compact: ## Manutenção: compacta + vacuum os small files do Silver (acelera o dashboard)
	docker compose run --rm --no-deps silver python -m live_telemetry.silver.compact

demo: up producer bronze gold ## Pipeline ponta a ponta
	@echo ""
	@echo "  dashboard: http://localhost:8501"
	@echo "  console:   http://localhost:8080"

test: ## Roda a suíte de testes unit (em container, monta o código atual)
	docker compose run --rm --no-deps -v $(abspath .):/app --entrypoint pytest silver -q

smoke: ## Smoke e2e isolado: gera→producer→bronze→gold→asserts (passa pelo broker) [SMOKE_N=300]
	@mkdir -p $(SMOKE_DIR)/raw
	docker run --rm \
	  -v $(abspath $(GEN_DIR)):/gen \
	  -v $(abspath $(SMOKE_DIR)/raw):/out \
	  -w /gen python:3.11-slim \
	  sh -c "pip install -q pyyaml pyarrow && python generate_all.py --n-sessions $(SMOKE_N) --output-dir /out"
	docker compose up -d redpanda
	docker compose --profile pipeline run --rm $(SMOKE_ENV) -e REPLAY_SPEED=1000 producer
	docker compose --profile batch run --rm $(SMOKE_ENV) bronze
	docker compose --profile batch run --rm $(SMOKE_ENV) gold
	docker compose run --rm --no-deps -v $(abspath .):/app $(SMOKE_ENV) -e SMOKE_E2E=1 \
	  --entrypoint pytest silver -q tests/test_smoke_e2e.py
	@echo "  ✅ smoke e2e OK (lake em $(SMOKE_DIR))"

logs: ## Tail dos serviços always-on
	docker compose logs -f silver streamlit

down: ## Derruba os serviços (mantém volumes)
	docker compose --profile pipeline --profile batch down

clean: ## Derruba + remove volumes (broker efêmero) + limpa o lake local
	docker compose --profile pipeline --profile batch down -v
	rm -rf data/bronze data/silver data/gold $(SMOKE_DIR)
