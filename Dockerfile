FROM python:3.10-slim

WORKDIR /app

# Set environment variables (users should provide these at runtime)
ENV DATABRICKS_HOST="***"
ENV DATABRICKS_TOKEN="***"

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "main.py"] 