FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENV PYTHONPATH=/app
CMD ["sh", "-c", "exec uvicorn services.synthetic_generator_service.app.main:app --host :: --port ${PORT:-8004}"]
