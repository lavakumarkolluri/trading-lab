FROM python:3.12-slim
WORKDIR /app

# Git SHA injected at build time: docker compose build --build-arg GIT_SHA=$(git rev-parse --short HEAD)
ARG GIT_SHA=unknown
ENV GIT_SHA=${GIT_SHA}
RUN echo "${GIT_SHA}" > /app/version.txt

# ── Install Docker CLI (needed by meta_pipeline.py to call docker compose) ──
# Only the CLI is installed — no daemon. The socket is bind-mounted at runtime.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        ca-certificates \
        gnupg \
    && install -m 0755 -d /etc/apt/keyrings \
    && curl -fsSL https://download.docker.com/linux/debian/gpg \
       | gpg --dearmor -o /etc/apt/keyrings/docker.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
       https://download.docker.com/linux/debian bookworm stable" \
       > /etc/apt/sources.list.d/docker.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends docker-ce-cli docker-compose-plugin git \
    && rm -rf /var/lib/apt/lists/*

# ── Python dependencies ──────────────────────────────────────────────────────
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Application code ─────────────────────────────────────────────────────────
# Copy all pipeline .py files at once — no per-file list to maintain.
# Research-only scripts (walk_forward_validation, momentum_factor_test,
# breakout_backtest) are harmlessly included; they just aren't used in prod.
COPY *.py .

CMD ["python", "main.py"]