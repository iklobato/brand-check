FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY config.py text.py models.py checks.py tasks.py server.py app.py ./
COPY templates/ ./templates/
ENV HOST=0.0.0.0 PORT=8080
EXPOSE 8080
CMD ["python", "app.py"]
