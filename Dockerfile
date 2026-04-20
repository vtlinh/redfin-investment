FROM python:3.13-slim

WORKDIR /app

RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY . .

EXPOSE 8080

CMD ["uv", "run", "--no-dev", "gunicorn", "webapp:app", \
     "--bind", "0.0.0.0:8080", "--workers", "2", "--timeout", "60"]
