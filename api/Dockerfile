FROM python:3.11-slim
WORKDIR /app
COPY api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY api/ .
COPY backfiller/__init__.py ./backfiller/__init__.py
COPY backfiller/roll_calendar.py ./backfiller/roll_calendar.py
COPY backfiller/roll_sync.py ./backfiller/roll_sync.py
COPY collector/config.py ./collector/config.py
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${API_PORT:-8000}"]
