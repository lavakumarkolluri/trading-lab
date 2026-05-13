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
COPY ch_utils.py .
COPY pipeline_utils.py .
COPY recompute.py .
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
COPY option_chain_intraday.py .
COPY option_chain_historical.py .
# Research/analysis scripts — run locally, not part of pipeline image
# COPY walk_forward_validation.py .
# COPY momentum_factor_test.py .
# COPY breakout_backtest.py .
COPY options_eod_summary_pipeline.py .
COPY compute_historical_iv.py .
COPY compute_oi_features.py .
COPY nifty_straddle_backtest.py .
COPY fo_utils.py .
COPY lot_size_pipeline.py .
COPY fundamental_pipeline.py .
COPY confidence_scorer.py .
COPY strategy_backtester.py .
COPY strategy_selector.py .
COPY events_pipeline.py .
COPY intraday_monitor.py .
COPY data_freshness_check.py .
COPY monthly_breakout_backtest.py .
COPY edge_analysis.py .
COPY weekend_theta_backtest.py .
COPY price_action.py .
COPY compute_greeks.py .
COPY compute_vol_surface.py .
COPY kite_orders.py .
COPY kite_auth.py .

CMD ["python", "main.py"]