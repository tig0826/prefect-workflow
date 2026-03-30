import json
from datetime import date, datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

from prefect import flow, task

from aw_exporter.aw_scraper import ActivityWatchScraper
from common.storage_tasks import save_json_to_s3
from common.trino_tasks import create_external, sync_table_partition


@task(retries=2, retry_delay_seconds=300, name="Scrape ActivityWatch Daily Data")
def scrape_aw_data(target_date: date) -> dict:
    """AWから指定日のデータをスクレイピングする"""
    scraper = ActivityWatchScraper(target_url="http://aw.mynet")
    return scraper.fetch_daily_data(target_date)


@flow(name="Life Metrics: ActivityWatch Exporter", log_prints=True)
def aw_flow(target_date: Optional[date] = None):
    """
    ActivityWatchデータ取得のメインフロー。
    スケジュール実行時は引数なしで呼ばれる。
    """
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
    # スクレイピング実行
    for d in target_dates:
        print(f"---{d} のデータを処理中 ---")
        try:
            # スクレイピング
            raw_data = scrape_aw_data(d)
            date_str = d.strftime("%Y-%m-%d")
            for bucket_name, events in raw_data.items():
                if not events:
                    continue  # 空のバケツは無視
                # バケツ名をそのままディレクトリ（テーブル）名として扱う
                jsonl_lines = [
                    json.dumps(event, ensure_ascii=False) for event in events
                ]
                jsonl_data = "\n".join(jsonl_lines) + "\n"
                save_json_to_s3(
                    data=jsonl_data,
                    prefix=f"aw/{bucket_name}/dt={date_str}",
                    file_name="data.jsonl",
                )
                print(f"Bronze: [{bucket_name}] に {len(events)} 件のデータを保存完了")
        except Exception as e:
            # ループの中でキャッチすることで、片方の日付がコケてももう片方は処理させる
            print(f"{d} の処理中にエラーが発生: {e}")
            has_error = True
    if has_error:
        raise RuntimeError("一部の日付の処理でエラーが発生")
    bucket_list = [
        "aw-watcher-afk_A1002995.local",
        "aw-watcher-afk_tignoMacBook-Pro.local",
        "aw-watcher-android-test",
        "aw-watcher-android-unlock",
        "aw-watcher-window_A1002995.local",
        "aw-watcher-window_tignoMacBook-Pro.local",
        "aw-watcher-window_DESKTOP-O8TFAG0",
        "aw-watcher-afk_DESKTOP-O8TFAG0",
    ]
    for bucket_name in bucket_list:
        clean_name = bucket_name.replace("-", "_").replace(".", "_").lower()
        clean_name = bucket_name.replace("aw-watcher-", "")
        if clean_name.endswith(".local"):
            clean_name = clean_name[:-6]
        clean_name = f"aw_{clean_name}".replace("-", "_").lower()
        aw_tablename = f"{clean_name}_external"
        create_external(
            system_name="aw",
            params={"aw_tablename": aw_tablename, "aw_bucketname": bucket_name},
        )
        sync_table_partition(table_name=aw_tablename)

    print("✅ 全ての対象日の処理が完了")


if __name__ == "__main__":
    # ローカルテスト用
    aw_flow()
