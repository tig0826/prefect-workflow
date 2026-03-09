# ステージ1: ビルダー (依存関係のインストール)
# uv公式イメージを使用することでセットアップ時間を短縮
FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim AS builder

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

WORKDIR /app

# 依存関係定義ファイルをコピー
COPY pyproject.toml uv.lock ./

# 依存関係をインストール
# --frozen: uv.lock の内容を厳守してインストール
# --no-dev: 開発用依存関係(ruff, ipython等)を除外
# --no-install-project: プロジェクト自体のコードはここではインストールしない
RUN uv sync --frozen --no-install-project --no-dev

# ステージ2: ランタイム (実行環境)
# ベースは軽量な slim イメージ
FROM python:3.11-slim-bookworm AS runtime

ENV PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH" \
    PYTHONPATH="/opt/prefect:$PYTHONPATH"

WORKDIR /opt/prefect

# 実行時に必要なシステムライブラリのみインストール
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    fonts-noto-cjk \
    wget \
    gnupg \
    ca-certificates \
 && wget -qO - https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /usr/share/keyrings/google-chrome-keyring.gpg \
 && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome-keyring.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list \
 && apt-get update \
 && apt-get install -y --no-install-recommends google-chrome-stable \
 && rm -rf /var/lib/apt/lists/* \
 && rm -f /etc/apt/sources.list.d/google-chrome.list

# ビルダーから作成済みの仮想環境を丸ごとコピー
COPY --from=builder /app/.venv /app/.venv

# アプリケーションコードをコピー
COPY get_itemname/get_itemname.py /opt/prefect/get_itemname/get_itemname.py
COPY get_item_hash_hiroba/get_item_hash.py /opt/prefect/get_item_hash_hiroba/get_item_hash.py
COPY get_trade_history/get_trade_history.py /opt/prefect/get_trade_history/get_trade_history.py
COPY get_price/get_price.py /opt/prefect/get_price/get_price.py
COPY get_price_mart/ /opt/prefect/get_price_mart
COPY create_llm_message/create_llm_message.py /opt/prefect/create_llm_message/create_llm_message.py
COPY create_llm_erudition_message/create_llm_erudition_message.py /opt/prefect/create_llm_erudition_message/create_llm_erudition_message.py
COPY get_momon_bazzar/ /opt/prefect/get_momon_bazzar
COPY post_diary/ /opt/prefect/post_diary
COPY keepalive_streamlit/ /opt/prefect/keepalive_streamlit
COPY sync_supabase_mrt_price_hourly /opt/prefect/sync_supabase_mrt_price_hourly
COPY notify_cookie_expiration/ /opt/prefect/notify_cookie_expiration
COPY common/ /opt/prefect/common

# dbt のプロファイル配置
ENV DBT_PROFILES_DIR=/opt/dbt
RUN mkdir -p /opt/dbt
COPY get_price_mart/infra/profiles.yml /opt/dbt/profiles.yml

# Prefect 用のメタデータ
LABEL io.prefect.version="3.1.11"
