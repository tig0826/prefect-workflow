FROM python:3.11-slim

RUN apt-get update && apt-get install -y gcc && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir discord.py requests prefect pandas trino

WORKDIR /app

# スクリプトをコピー
COPY discord_price_bot.py .

# スクリプトを実行
CMD ["python", "discord_price_bot.py"]
