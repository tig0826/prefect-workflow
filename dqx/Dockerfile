# ---- builder ----
FROM python:3.11-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential gcc python3-dev libpq-dev \
  && rm -rf /var/lib/apt/lists/*

ENV PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=on
WORKDIR /src

# 実際に使う pyproject を置く
COPY pyproject.toml pyproject.toml
COPY poetry.lock* .

RUN pip install --upgrade pip wheel poetry \
 && poetry --version \
 && poetry self add "poetry-plugin-export>=1.7.0"

# lock を pyproject に同期（依存を更新せず差分だけ反映）
RUN poetry lock

RUN poetry export -f requirements.txt --without-hashes -o /src/requirements.txt

# 依存を wheel 化
RUN pip wheel -r requirements.txt -w /wheels


## --------------------------------------------------------------

FROM python:3.11.6-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=on

# 実行時に必要なランタイム依存だけ（libpq など）
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    fonts-noto-cjk \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/prefect

# builder から完成済み wheel と requirements.txt を受け取り、オフラインでインストール
COPY --from=builder /wheels /wheels
COPY --from=builder /src/requirements.txt /requirements.txt
RUN pip install --no-index --find-links=/wheels -r /requirements.txt \
 && rm -rf /wheels /root/.cache/pip
# RUN pip install --no-index --find-links=/wheels -r /requirements.txt

# アプリコードだけをコピー（余計な build ツールやキャッシュは持ち込まない）
# 既存構成に合わせてそのまま転記
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

# dbt のプロファイル配置（必要に応じて環境変数で上書き可）
ENV DBT_PROFILES_DIR=/opt/dbt
RUN mkdir -p /opt/dbt
COPY get_price_mart/infra/profiles.yml /opt/dbt/profiles.yml

# Prefect 用のメタデータ（必要なら残す）
LABEL io.prefect.version="3.4.11"




