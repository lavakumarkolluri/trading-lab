FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir \
    yfinance \
    pandas \
    numpy \
    pyarrow \
    minio \
    clickhouse-connect \
    boto3 \
    requests \
    schedule
COPY main.py .
COPY symbols.py .
COPY mf_pipeline.py .
COPY mf_aggregation.py .
COPY cleanup_schemes.py .
CMD ["python", "main.py"]