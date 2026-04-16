FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
ARG CACHE_BUST=2026-04-16-01
RUN echo "CACHE_BUST=${CACHE_BUST}"
COPY . .
CMD ["python", "bot.py"]
