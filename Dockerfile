# Dockerfile
FROM python:3.13-slim

WORKDIR /app

# Устанавливаем зависимости и пакет
COPY pyproject.toml .
RUN pip install --no-cache-dir -e .

# Копируем весь код
COPY . .

# Порт для Dagster
EXPOSE 3000

CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000"]