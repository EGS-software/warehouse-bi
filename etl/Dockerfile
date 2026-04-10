# 1. Imagem base oficial do Python
FROM python:latest

# 2. Define o diretório de trabalho dentro do container
WORKDIR /app

# 3. Copia apenas o requirements.txt para aproveitar o cache de camadas
COPY requirements.txt .

# 4. Instala as dependências
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copia o restante do código da aplicação
COPY . .

# 6. Comando para executar a aplicação
CMD ["python", "main.py"]
