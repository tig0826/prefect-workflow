import json
from datetime import date, datetime, timedelta
from typing import Optional

from prefect import flow, task
from prefect.blocks.system import Secret

from asken_exporter.asken_scraper import AskenScraper
from common.storage_tasks import save_json_to_s3
from common.trino_tasks import create_external, sync_table_partition


@task(retries=2, retry_delay_seconds=60, name="Fetch Asken Credentials")
def get_credentials() -> tuple[str, str]:
    """
    PrefectのSecret Blockからあすけんの認証情報を安全に取得する。
    """
    email = Secret.load("asken-email").get()
    password = Secret.load("asken-password").get()
    return email, password


@task(retries=1, retry_delay_seconds=300, name="Scrape Asken Daily Data")
def scrape_asken_data(email: str, password: str, target_date: date) -> dict:
    """あすけんから指定日のデータをスクレイピングする"""
    scraper = AskenScraper(email, password)
    return scraper.fetch_daily_data(target_date)


@flow(name="Life Metrics: Asken Exporter", log_prints=True)
def asken_flow(target_date: Optional[date] = None):
    """
    あすけんデータ取得のメインフロー。
    スケジュール実行時は引数なしで呼ばれ、「昨日」のデータを取得する。
    """
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
            print(
                f"🌅 朝の定期実行(現在{current_hour}時)を検知。昨日のデータ({yesterday})も再取得対象に追加する。"
            )

    print(f"🚀 あすけんデータ抽出パイプラインを開始。対象日: {target_dates}")
    # 認証情報の取得
    email, password = get_credentials()
    has_error = False
    # スクレイピング実行
    for d in target_dates:
        print(f"--- 📅 {d} のデータを処理中 ---")
        try:
            # スクレイピング
            raw_data = scrape_asken_data(email, password, d)
            jsonl_data = json.dumps(raw_data, ensure_ascii=False) + "\n"
            if raw_data:
                # S3への保存 (ファイル名に日付が入るので上書きの心配はない)
                date_str = d.strftime("%Y-%m-%d")
                save_json_to_s3(
                    data=jsonl_data,
                    prefix=f"asken/raw/dt={date_str}",
                    file_name="data.jsonl",
                )

            else:
                print(f"⚠️ {d} のデータが空だったためスキップ")
        except Exception as e:
            # ループの中でキャッチすることで、片方の日付がコケてももう片方は処理させる
            print(f"❌ {d} の処理中にエラーが発生: {e}")
            has_error = True
    if has_error:
        raise RuntimeError("⚠️ 一部の日付の処理でエラーが発生")

    create_external(system_name="asken")
    sync_table_partition(table_name="asken_external")

    print("✅ 全ての対象日の処理が完了")


if __name__ == "__main__":
    # ローカルテスト用
    # 実行する前に `prefect block register` や UI で Secret を作っておけよ。
    asken()
