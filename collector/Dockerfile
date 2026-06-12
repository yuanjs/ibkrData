FROM python:3.11-slim
WORKDIR /app
COPY collector/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY collector/ .
COPY backfiller/ ./backfiller/
CMD ["python", "main.py"]
