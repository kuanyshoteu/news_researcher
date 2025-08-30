FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Системные зависимости для psycopg
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# Зависимости
RUN pip install --no-cache-dir \
    fastapi uvicorn[standard] \
    sqlalchemy==2.0.32 \
    psycopg[binary]==3.2.1 \
    pydantic==2.8.2 pydantic-settings==2.4.0
    babel==2.17.0
    certifi==2025.8.3
    charset-normalizer==3.4.3
    courlan==1.3.2
    dateparser==1.2.2
    feedparser==6.0.11
    htmldate==1.9.3
    idna==3.10
    jusText==3.0.2
    lxml==5.4.0
    lxml_html_clean==0.4.2
    python-dateutil==2.9.0.post0
    pytz==2025.2
    PyYAML==6.0.2
    regex==2025.7.34
    requests==2.32.5
    sgmllib3k==1.0.0
    six==1.17.0
    tld==0.13.1
    trafilatura==2.0.0
    tzdata==2025.2
    tzlocal==5.3.1
    urllib3==2.5.0


COPY . .

# Запускаем API
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
