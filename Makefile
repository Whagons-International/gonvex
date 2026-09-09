.PHONY: dev stack stack-storage services storage files runtime dashboard packages docs version release-test release-notes-preview release-dry-run release-prod

OPENROUTER_MODEL ?= moonshotai/kimi-k2.5

dev:
	@bash -lc 'set -euo pipefail; \
		if [ -f .env ]; then set -a; source .env; set +a; fi; \
		export GONVEX_SANDBOX_ENABLED="$${GONVEX_SANDBOX_ENABLED:-true}"; \
		export GONVEX_SANDBOX_ALLOW_UNCONFINED="$${GONVEX_SANDBOX_ALLOW_UNCONFINED:-true}"; \
		runtime_pid=""; dashboard_pid=""; packages_pid=""; \
		cleanup() { \
			[ -n "$$runtime_pid" ] && kill -- "-$$runtime_pid" 2>/dev/null || true; \
			[ -n "$$dashboard_pid" ] && kill -- "-$$dashboard_pid" 2>/dev/null || true; \
			[ -n "$$packages_pid" ] && kill -- "-$$packages_pid" 2>/dev/null || true; \
		}; \
		trap cleanup EXIT INT TERM; \
		pnpm build:packages; \
		setsid pnpm dev:packages & packages_pid=$$!; \
		setsid pnpm dev:runtime & runtime_pid=$$!; \
		setsid pnpm dev:dashboard & dashboard_pid=$$!; \
		while kill -0 "$$packages_pid" 2>/dev/null \
			&& kill -0 "$$runtime_pid" 2>/dev/null \
			&& kill -0 "$$dashboard_pid" 2>/dev/null; do \
			sleep 1; \
		done'

services:
	docker compose -f infra/docker-compose.dev.yml up -d --wait postgres valkey

stack:
	docker compose up -d --build --wait postgres valkey minio runtime dashboard
	docker compose run --rm minio-init

stack-storage: stack

storage:
	docker compose -f infra/docker-compose.dev.yml --profile storage up -d --wait minio
	docker compose -f infra/docker-compose.dev.yml --profile storage run --rm minio-init

files: storage

runtime:
	pnpm dev:runtime

dashboard:
	pnpm dev:dashboard

packages:
	pnpm dev:packages

docs:
	pnpm dev:docs

version:
	node scripts/release-cli.mjs --version-info

release-test:
	node --test scripts/release-cli.test.mjs

release-notes-preview:
	OPENROUTER_MODEL="$(OPENROUTER_MODEL)" VERSION="$(VERSION)" node scripts/release-cli.mjs --notes-preview

release-dry-run:
	OPENROUTER_MODEL="$(OPENROUTER_MODEL)" VERSION="$(VERSION)" node scripts/release-cli.mjs --dry-run

release-prod:
	OPENROUTER_MODEL="$(OPENROUTER_MODEL)" VERSION="$(VERSION)" node scripts/release-cli.mjs
