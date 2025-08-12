FROM python:3.12-slim

# 装构建 psycopg2 所需：pg_config 在 libpq-dev 里
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential gcc libpq-dev ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . ./

ENV PORT=5000
EXPOSE 5000

CMD ["gunicorn", "app:app", "-b", "0.0.0.0:5000", "--workers", "2", "--threads", "4", "--timeout", "120", "--preload"]