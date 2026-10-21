FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app/src
ARG INSTALL_KAFKA=false
WORKDIR /app

COPY requirements.txt requirements-kafka.txt ./
RUN if [ "$INSTALL_KAFKA" = "true" ]; then pip install --no-cache-dir -r requirements-kafka.txt; \
    else pip install --no-cache-dir -r requirements.txt; fi

COPY config ./config
COPY sql ./sql
COPY src ./src
COPY scripts ./scripts
COPY data/reference ./data/reference
COPY data/sample ./data/sample

RUN useradd --create-home --uid 10001 rideflow && mkdir -p data/lake data/warehouse reports \
    && chown -R rideflow:rideflow /app
USER rideflow

HEALTHCHECK --interval=60s --timeout=10s --start-period=5s --retries=3 \
  CMD python -m rideflow health || exit 1

CMD ["python", "-m", "rideflow", "run"]
