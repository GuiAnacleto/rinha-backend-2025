FROM pypy:3.10-slim

RUN apt update && apt install -y build-essential

RUN apt-get update && apt-get install -y ca-certificates && update-ca-certificates

# Cria o diretório de trabalho no container
WORKDIR /app

# Copia o requirements adaptado para PyPy
COPY requirements_pypy.txt .

# Instala dependências compatíveis com PyPy
RUN pypy3 -m pip install --no-cache-dir -r requirements_pypy.txt

# Copia todos os arquivos do projeto para o container
COPY . .

# Expõe a porta 9999
EXPOSE 9999

# Executa o script principal com PyPy
CMD ["pypy3", "app/run.py"]