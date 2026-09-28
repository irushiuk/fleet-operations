FROM python:3.11-slim
WORKDIR /app
COPY producers/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY common/ /app/common/
COPY producers/ /app/producers/
ENV PYTHONUNBUFFERED=1
