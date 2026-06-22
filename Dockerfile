# Imagem única para todos os serviços Python; o comando é sobrescrito por serviço no compose.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Deps primeiro (cache de layer)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Código + instalação editável do pacote
COPY pyproject.toml .
COPY src ./src
RUN pip install --no-cache-dir -e .

# Artefatos de runtime copiados no build final (no dev vêm via bind-mount)
COPY CONTRACTS ./CONTRACTS
COPY OBSERVABILITY ./OBSERVABILITY
COPY dbt ./dbt

CMD ["python", "-c", "print('especifique um comando no compose')"]
