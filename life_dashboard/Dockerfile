FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

WORKDIR /app

# 依存関係定義ファイルをコピー
COPY pyproject.toml uv.lock ./

RUN uv sync --frozen --no-install-project --no-dev

FROM python:3.12-slim-bookworm AS runtime

ENV PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH" \
    PYTHONPATH="/opt/prefect:$PYTHONPATH"

WORKDIR /opt/prefect

# 実行時に必要なシステムライブラリのみインストール
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    fonts-noto-cjk \
  && rm -rf /var/lib/apt/lists/*

RUN groupadd -g 1000 prefectgroup && \
    useradd -u 1000 -g prefectgroup -s /bin/bash -m prefectuser

# ビルダーから作成済みの仮想環境を丸ごとコピー
COPY --from=builder --chown=prefectuser:prefectgroup /app/.venv /app/.venv

# アプリケーションコードをコピー
COPY --chown=prefectuser:prefectgroup ./common /opt/prefect/common
COPY --chown=prefectuser:prefectgroup ./asken_exporter/ /opt/prefect/asken_exporter
COPY --chown=prefectuser:prefectgroup ./fitbit_exporter/ /opt/prefect/fitbit_exporter
COPY --chown=prefectuser:prefectgroup ./aw_exporter/ /opt/prefect/aw_exporter
COPY --chown=prefectuser:prefectgroup ./owntracks_exporter/ /opt/prefect/owntracks_exporter
COPY --chown=prefectuser:prefectgroup ./dbt_lifeos/ /opt/prefect/dbt_lifeos

# dbt のプロファイル配置
# ENV DBT_PROFILES_DIR=/opt/dbt
# RUN mkdir -p /opt/dbt
# COPY get_price_mart/infra/profiles.yml /opt/dbt/profiles.yml

# 実行ユーザーの切り替え
USER prefectuser

# Prefect 用のメタデータ
LABEL io.prefect.version="3.1.11" \
      maintainer="HomeServer_Admin"
