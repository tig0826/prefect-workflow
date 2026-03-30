import json
import logging
from datetime import date, datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

from prefect import flow, task
from prefect.blocks.system import Secret

from fitbit_exporter.fitbit_scraper import FitbitScraper
from common.storage_tasks import save_json_to_s3
from common.trino_tasks import create_external, sync_table_partition

TOKEN_BLOCK_NAME = "fitbit-token"


def update_prefect_token_block(new_token: dict):
    """
    FitbitScraperに注入するコールバック関数。
    トークンが更新されたら、JSON文字列に変換して Prefect の Secret Block を上書き保存する。
    """
    logging.info("[Callback] トークンがリフレッシュされたため、Prefect Secretを更新。")
    try:
        token_str = json.dumps(new_token)
        secret_block = Secret(value=token_str)
        secret_block.save(name=TOKEN_BLOCK_NAME, overwrite=True)
        logging.info("新しいトークンを Secret として安全に保存完了。")
    except Exception as e:
        logging.error(f"トークンの保存に失敗: {e}")


@task(retries=2, retry_delay_seconds=60, name="Prepare Fitbit Credentials")
def get_credentials() -> tuple[str, str, dict]:
    """PrefectからAPIキーと現在のトークンを取得する"""
    client_id = Secret.load("fitbit-client-id").get()
    client_secret = Secret.load("fitbit-client-secret").get()
    try:
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
        raise ValueError(
            f"Secret '{TOKEN_BLOCK_NAME}' の中身が正しいJSON形式ではない。"
        )

    return client_id, client_secret, current_token


@task(retries=1, retry_delay_seconds=300, name="Scrape Fitbit Daily Data")
def scrape_fitbit_data(
    client_id: str, client_secret: str, current_token: dict, target_date: date
) -> dict:
    """スクレイパーを初期化し、生データを辞書で取得する"""
    scraper = FitbitScraper(
        client_id=client_id,
        client_secret=client_secret,
        token_dict=current_token,
        refresh_cb=update_prefect_token_block,
    )
    # ここは単なる生の辞書を返す想定
    return scraper.fetch_daily_data(target_date)


@flow(name="Life Metrics: Fitbit Exporter", log_prints=True)
def fitbit_flow(target_date: Optional[date] = None):
    """Fitbitデータ取得のメインフロー"""
    client_id, client_secret, current_token = get_credentials()

    target_dates: list[date] = []
    if target_date:
        target_dates.append(target_date)
    else:
        jst_now = datetime.now(ZoneInfo("Asia/Tokyo"))
        today = jst_now.date()
        target_dates.extend([today - timedelta(days=1), today])
    print(
        f"対象日付: {[d.strftime('%Y-%m-%d') for d in target_dates]} のFitbitデータ抽出を開始..."
    )

    has_error = False

    for d in target_dates:
        date_str = d.strftime("%Y-%m-%d")
        print(f"--- {date_str} のデータを処理中 ---")
        try:
            raw_data = scrape_fitbit_data(client_id, client_secret, current_token, d)

            # データが存在するかチェック（dateキー以外に値があるか）
            if raw_data and any(bool(v) for k, v in raw_data.items() if k != "date"):
                # 🌟 極めて重要: TrinoのBronzeテーブル(raw_json, dt)に合わせたラッパーを作成
                wrapped_payload = {
                    "raw_json": json.dumps(raw_data, ensure_ascii=False),
                    "dt": date_str,
                }

                # JSONL形式（1行）としてシリアライズ
                jsonl_data = json.dumps(wrapped_payload, ensure_ascii=False) + "\n"

                save_json_to_s3(
                    data=jsonl_data,
                    prefix=f"fitbit/raw/dt={date_str}",
                    file_name="data.jsonl",
                )
                print(f"{date_str} のデータをS3に保存完了。")
            else:
                print(f"{date_str} のデータが空だったため保存をスキップ。")

        except Exception as e:
            print(f"{date_str} の処理中にエラーが発生: {e}")
            has_error = True

    # 外部テーブルとパーティションの同期
    # (エラーがあっても、成功した日があるかもしれないので同期は実行しておくのが定石だ)
    try:
        create_external(system_name="fitbit")
        sync_table_partition(table_name="fitbit_external")
    except Exception as e:
        print(f"Trinoのメタデータ同期中にエラーが発生: {e}")
        has_error = True

    # 全て終わった最後に、エラーフラグが立っていればタスクを失敗状態にする
    if has_error:
        raise RuntimeError(
            "フロー実行中に一部のエラーが発生しました。ログを確認してください。"
        )


if __name__ == "__main__":
    fitbit_flow()
