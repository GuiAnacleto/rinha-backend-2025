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

# Expõe a porta 8080 (opcional)
EXPOSE 8080

# Executa o script principal
CMD ["python", "app/run.py"]
