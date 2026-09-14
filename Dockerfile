FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .
COPY wallet_monitor.py .
COPY tracked_wallets.json .
ENV PYTHONUNBUFFERED=1
CMD ["python", "app.py"]
