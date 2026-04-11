# Usa uma imagem oficial e leve do Python
FROM python:3.9-slim

# Define a pasta de trabalho dentro do contêiner
WORKDIR /app

# Instala dependências do sistema operacional necessárias para o psycopg2 (driver do Postgres)
RUN apt-get update && apt-get install -y libpq-dev gcc

# Copia o arquivo de dependências e instala
# OBS: Crie um arquivo requirements.txt na pasta etl com: pandas, sqlalchemy, psycopg2-binary
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o resto dos arquivos
COPY . .

# Comando que será executado quando o contêiner subir
CMD ["python", "main.py"]