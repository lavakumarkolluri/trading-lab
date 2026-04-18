FROM python:3.12-slim
WORKDIR /app

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
    && apt-get install -y --no-install-recommends docker-ce-cli \
    && rm -rf /var/lib/apt/lists/*

# ── Python dependencies ──────────────────────────────────────────────────────
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Application code ─────────────────────────────────────────────────────────
COPY main.py .
COPY symbols.py .
COPY mf_pipeline.py .
COPY mf_aggregation.py .
COPY mf_db.py .
COPY mf_compute_technical.py .
COPY mf_compute_returns.py .
COPY mf_compute_risk.py .
COPY cleanup_schemes.py .
COPY event_detector.py .
COPY pattern_feature_extractor.py .
COPY pattern_builder.py .
COPY backtest_engine.py .
COPY star_rater.py .
COPY prediction_engine.py .
COPY validation_engine.py .
COPY meta_pipeline.py .
COPY holidays_pipeline.py .
COPY fii_dii_pipeline.py .
COPY participant_oi_pipeline.py .
COPY gap_analyzer.py .
COPY scheduler.py .
COPY vix_pipeline.py .
COPY option_chain_pipeline.py .
COPY option_backtest.py .

CMD ["python", "main.py"]