FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY bridge ./bridge
COPY config ./config
COPY README.md .
RUN mkdir -p /app/downloads

CMD ["python", "-m", "bridge.web"]
