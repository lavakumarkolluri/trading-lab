FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    yfinance \
    pandas \
    pyarrow \
    minio \
    clickhouse-connect \
    boto3 \
    requests \
    schedule

COPY main.py .

CMD ["python", "main.py"]