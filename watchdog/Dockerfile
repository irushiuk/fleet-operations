FROM python:3.11-slim
WORKDIR /app
COPY watchdog/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY common/ /app/common/
COPY watchdog/ /app/watchdog/
ENV PYTHONUNBUFFERED=1
CMD ["python", "watchdog/watchdog.py"]
