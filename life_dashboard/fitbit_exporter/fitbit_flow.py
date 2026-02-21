import sys
import os
import json
import logging
from datetime import date, timedelta
from typing import Optional

from prefect import flow, task
from prefect.blocks.system import Secret

from fitbit_exporter.fitbit_scraper import FitbitScraper
from common.storage import save_json_to_s3

TOKEN_BLOCK_NAME = "fitbit-token"

def update_prefect_token_block(new_token: dict):
    """
    FitbitScraperに注入するコールバック関数。
    トークンが更新されたら、JSON文字列に変換して Prefect の Secret Block を上書き保存する。
    """
    logging.info("🔄 [Callback] トークンがリフレッシュされたため、Prefect Secretを更新。")
    try:
        # 辞書をJSON文字列に変換
        token_str = json.dumps(new_token)
        # Secretブロックとして初期化して保存 (overwrite=Trueで上書き)
        secret_block = Secret(value=token_str)
        secret_block.save(name=TOKEN_BLOCK_NAME, overwrite=True)
        logging.info("✅ 新しいトークンを Secret として安全に保存完了。")
    except Exception as e:
        logging.error(f"❌ トークンの保存に失敗: {e}")

@task(retries=2, retry_delay_seconds=60, name="Prepare Fitbit Credentials")
def get_credentials() -> tuple[str, str, dict]:
    """PrefectからAPIキーと現在のトークンを取得する"""
    client_id = Secret.load("fitbit-client-id").get()
    client_secret = Secret.load("fitbit-client-secret").get()
    try:
        # Secretから暗号化されたJSON文字列を取り出し、辞書に復元する
        raw_token = Secret.load(TOKEN_BLOCK_NAME).get()
        if isinstance(raw_token, dict):
            current_token = raw_token
        elif isinstance(raw_token, str):
            current_token = json.loads(raw_token)
        else:
            raise ValueError(f"トークンの型が不正: {type(raw_token)}")
    except ValueError:
        raise FileNotFoundError(f"Prefect Secret '{TOKEN_BLOCK_NAME}' が見つからない。")
    except json.JSONDecodeError:
        raise ValueError(f"Secret '{TOKEN_BLOCK_NAME}' の中身が正しいJSON形式ではない。")
    return client_id, client_secret, current_token

@task(retries=1, retry_delay_seconds=300, name="Scrape Fitbit Daily Data")
def scrape_fitbit_data(client_id: str, client_secret: str, current_token: dict, target_date: date) -> dict:
    """スクレイパーを初期化し、データを取得する"""
    # ここでコールバック関数を渡す (Dependency Injection)
    scraper = FitbitScraper(
        client_id=client_id, 
        client_secret=client_secret, 
        token_dict=current_token, 
        refresh_cb=update_prefect_token_block
    )
    return scraper.fetch_daily_data(target_date)

@flow(name="Life Metrics: Fitbit Exporter", log_prints=True)
def fitbit_flow(target_date: Optional[date] = None):
    """Fitbitデータ取得のメインフロー"""
    if target_date is None:
        target_date = date.today() - timedelta(days=1)

    print(f"🚀 {target_date} のFitbitデータ抽出パイプラインを開始...")

    # 1. 認証情報と状態(トークン)の取得
    client_id, client_secret, current_token = get_credentials()

        # 取得対象の日付リストを作成
    target_dates: List[date] = []

    if target_date:
        target_dates.append(target_date)
    else:
        today = date.today()
        target_dates.append(today)
        # これにより、深夜に記録した夜食データを朝イチで確実に回収する。
        current_hour = datetime.now().hour
        if current_hour < 10:
            yesterday = today - timedelta(days=1)
            target_dates.append(yesterday)
            print(f"🌅 朝の定期実行(現在{current_hour}時)を検知。昨日のデータ({yesterday})も再取得対象に追加する。")
    has_error = False
    # スクレイピング実行
    for d in target_dates:
        print(f"--- 📅 {d} のデータを処理中 ---")
        try:
            # スクレイピング
            raw_data = scrape_fitbit_data(client_id, client_secret, current_token, d)
            if raw_data and any(bool(v) for k, v in raw_data.items() if k != "date"):
                # S3への保存
                date_str = d.strftime("%Y-%m-%d")
                file_name = f"fitbit_{date_str}.json"
                save_json_to_s3(data=raw_data, prefix="fitbit/raw", file_name=file_name)
            else:
                print(f"⚠️ {d} のデータが空だったためスキップ")
                has_error = True
        except Exception as e:
            # ループの中でキャッチすることで、片方の日付がコケてももう片方は処理させる
            print(f"❌ {d} の処理中にエラーが発生: {e}")
            has_error = True
    if has_error:
        raise RuntimeError("⚠️ 一部の日付の処理でエラーが発生")


if __name__ == "__main__":
    fitbit_flow()
