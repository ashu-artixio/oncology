FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    VIRTUAL_ENV=/opt/venv

RUN python -m venv "${VIRTUAL_ENV}" \
    && . "${VIRTUAL_ENV}/bin/activate" \
    && pip install --upgrade pip

ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

RUN mkdir -p /app/temp/mondo_cache

ENTRYPOINT ["python", "app.py"]
CMD ["mondo_ingest", "--config", "/app/config.json"]

