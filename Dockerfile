FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .
COPY wallet_monitor.py .
COPY lore_monitor.py .
COPY social_monitor.py .
COPY exchange_monitor.py .
COPY research_gate.py .
COPY tracked_wallets.json .
COPY social_watchlist.json .
ENV PYTHONUNBUFFERED=1
CMD ["python", "app.py"]
