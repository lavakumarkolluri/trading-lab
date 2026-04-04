FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
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
CMD ["python", "main.py"]