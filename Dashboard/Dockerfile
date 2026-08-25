# Read-only onboarding dashboard — Cloud Run (Flask + gunicorn).
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App code + templates/static, plus firestore_store.py (copied from ../Onboarding
# at deploy time — single source of truth for the DB/collection + doc shape).
COPY . .

# Cloud Run provides $PORT. gunicorn serves the Flask `app` object in app.py.
CMD exec gunicorn --bind :$PORT --workers 2 --threads 8 --timeout 120 app:app