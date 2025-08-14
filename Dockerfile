FROM python:3.10-slim

RUN apt-get update && apt-get install -y ca-certificates && update-ca-certificates

# Cria o diretório de trabalho no container
WORKDIR /app

# Copia o requirements.txt (se existir)
COPY requirements.txt .

# Instala dependências (ignora erro se não houver)
RUN pip install --no-cache-dir -r requirements.txt || true

# Copia todos os arquivos do projeto para o container
COPY . .

# Executa o script principal
CMD ["uvicorn", "app.app:app", "--host", "0.0.0.0", "--port", "9999"]