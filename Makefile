COMPOSE := docker compose -f /trading-lab/docker-compose.yml --project-directory /trading-lab
HOST_COMPOSE := docker compose -f $(PWD)/docker-compose.yml

# ── Deploy ────────────────────────────────────────────────────────────────────
# Full deploy: pull latest code, rebuild all images, prune stale layers.
# Use after pushing to stage and CI has merged to master.
.PHONY: deploy
deploy:
	git pull origin master
	$(HOST_COMPOSE) build
	docker image prune -f
	@echo "Deploy complete. Restart scheduler if scheduler.py changed:"
	@echo "  make restart SVC=scheduler"

# Rebuild a single service image (e.g. make rebuild SVC=option_chain_historical)
.PHONY: rebuild
rebuild:
	$(HOST_COMPOSE) build $(SVC)
	docker image prune -f

# ── Run once ──────────────────────────────────────────────────────────────────
# Run a pipeline once and stream output (e.g. make run SVC=meta_pipeline)
.PHONY: run
run:
	$(HOST_COMPOSE) run --rm $(SVC)

# Run with extra args (e.g. make runargs SVC=meta_pipeline ARGS="--weekly")
.PHONY: runargs
runargs:
	$(HOST_COMPOSE) run --rm $(SVC) $(ARGS)

# ── Ops ───────────────────────────────────────────────────────────────────────
.PHONY: logs
logs:
	docker logs $(SVC) --tail=100 -f

.PHONY: restart
restart:
	$(HOST_COMPOSE) restart $(SVC)

.PHONY: ps
ps:
	$(HOST_COMPOSE) ps

.PHONY: freshness
freshness:
	$(HOST_COMPOSE) run --rm data_freshness_check

# ── Tests ─────────────────────────────────────────────────────────────────────
.PHONY: test
test:
	python3 -m pytest tests/ -q
