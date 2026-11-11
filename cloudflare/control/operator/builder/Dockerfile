# The K19 release builder as a Container (0028 §3.1): the same server.mjs the
# local kernel runs, with the kernel's sources so it can build and deploy the
# Worker version (GONVEX_BUILDER_TARGET=wrangler). Build from the repository root:
#
#   docker build -f cloudflare/control/operator/builder/Dockerfile -t gonvex-cf-dev-k19-builder .
#
# Runtime configuration (Container env / secrets): GONVEX_RELEASE_BUILDER_TOKEN,
# GONVEX_KERNEL_URL, GONVEX_BUILDER_TARGET=wrangler, GONVEX_KERNEL_WRANGLER_CONFIG,
# GONVEX_KERNEL_SCRIPT, CLOUDFLARE_ACCOUNT_ID, CLOUDFLARE_API_TOKEN.
# Wiring it as a Container binding needs K20's infrastructure and the account
# (follow-up in 0036).
FROM node:22.18-bookworm-slim
WORKDIR /gonvex
RUN corepack enable && corepack prepare pnpm@11.1.2 --activate
COPY pnpm-workspace.yaml pnpm-lock.yaml package.json ./
COPY cloudflare/package.json cloudflare/package.json
RUN pnpm install --frozen-lockfile --filter @gonvex/cloudflare
COPY rust/crates/store/src/control_schema.sql rust/crates/store/src/control_schema.sql
COPY cloudflare cloudflare
ENV GONVEX_BUILDER_HOST=0.0.0.0 GONVEX_BUILDER_PORT=8790 NODE_OPTIONS=--max-old-space-size=1536
EXPOSE 8790
CMD ["node", "cloudflare/control/operator/builder/server.mjs"]
