"""Google Health API から体重・体脂肪・歩数・睡眠を取って bronze に置く。

レガシー Fitbit Web API の後継。2026年9月に旧APIが止まるまでは
fitbit_flow と並行稼働させる（停止日が月内のどこかで未公表なので、
切れる瞬間に穴を空けないため）。

**fitbit_flow から持ち込まない設計:**
fitbit_flow は対象日を「昨日と今日」に限っている。そのため Fitbit 側に
後から入った値や、その時だけ API が失敗した日が永久に欠けたままになる。
実例が 2026-08-23 の体重で、今 API を叩けば 92.9kg が返るのに bronze は
null のままだ。silver は直近14日を読み直して自己修復する作りになっているが、
bronze が更新されないので効かない。ここでは既定で数日分を毎回引き直す。
"""

import json
import logging
from datetime import date, datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

from prefect import flow, task
from prefect.blocks.system import Secret

from common.storage_tasks import save_json_to_s3
from common.trino_tasks import create_external, sync_table_partition
from google_health_exporter.google_health_scraper import DATA_TYPES, GoogleHealthScraper

TOKEN_BLOCK_NAME = "google-health-token"
CLIENT_ID_BLOCK_NAME = "google-health-client-id"
CLIENT_SECRET_BLOCK_NAME = "google-health-client-secret"

# 何日分を毎回引き直すか。1日1ファイルの上書きなのでコストは無視できる。
DEFAULT_LOOKBACK_DAYS = 5


def update_prefect_token_block(new_token: dict) -> None:
    """Google が refresh_token を差し替えてきたときだけ呼ばれる。

    通常は差し替わらないが、黙って古いものを持ち続けると次回以降
    invalid_grant で落ちて、しかも原因が分かりにくい。
    """
    logging.info("[Callback] refresh_token が差し替わったので Secret を更新する")
    try:
        Secret(value=json.dumps(new_token)).save(name=TOKEN_BLOCK_NAME, overwrite=True)
        logging.info("新しいトークンを保存した")
    except Exception as e:
        logging.error(f"トークンの保存に失敗: {e}")


@task(retries=2, retry_delay_seconds=60, name="Prepare Google Health Credentials")
def get_credentials() -> tuple[str, str, dict]:
    client_id = Secret.load(CLIENT_ID_BLOCK_NAME).get()
    client_secret = Secret.load(CLIENT_SECRET_BLOCK_NAME).get()
    raw = Secret.load(TOKEN_BLOCK_NAME).get()
    if isinstance(raw, dict):
        token = raw
    elif isinstance(raw, str):
        token = json.loads(raw)
    else:
        raise ValueError(f"トークンの型が不正: {type(raw)}")
    if "refresh_token" not in token:
        raise ValueError(
            f"Secret '{TOKEN_BLOCK_NAME}' に refresh_token が無い。"
            "google_health_exporter.get_google_health_token を実行し直すこと。"
        )
    return client_id, client_secret, token


@task(retries=1, retry_delay_seconds=300, name="Scrape Google Health Range")
def scrape_range(
    client_id: str, client_secret: str, token: dict, start: date, end: date
) -> dict:
    scraper = GoogleHealthScraper(
        client_id=client_id,
        client_secret=client_secret,
        token_dict=token,
        refresh_cb=update_prefect_token_block,
    )
    return scraper.fetch_range(start, end)


@flow(name="Life Metrics: Google Health Exporter", log_prints=True)
def google_health_flow(
    target_date: Optional[date] = None,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
):
    """target_date を渡せばその1日だけ、渡さなければ直近 lookback_days 日分。"""
    client_id, client_secret, token = get_credentials()

    if target_date:
        start = end = target_date
    else:
        today = datetime.now(ZoneInfo("Asia/Tokyo")).date()
        start = today - timedelta(days=lookback_days - 1)
        end = today
    print(f"対象期間 (JST): {start} 〜 {end}")

    buckets = scrape_range(client_id, client_secret, token, start, end)

    has_error = False
    saved = 0
    for date_str in sorted(buckets):
        payload = buckets[date_str]

        # 取得に失敗したデータ型は None になっている。空リストと区別すること。
        # None（取得失敗）と [] （その日に本当にデータが無い）を混同して
        # 保存すると、正常なパーティションを空で上書きしてしまう。
        # fitbit_flow で 0歩・0kcal が恒久的に凍結したのと同じ事故になる。
        failed = [k for k, v in payload.items() if v is None]
        if failed:
            print(f"{date_str}: 取得できなかったデータ型 {failed}")
            has_error = True

        present = {k: v for k, v in payload.items() if v}
        if not present:
            print(f"{date_str}: 中身が無いので保存をスキップ")
            continue

        counts = {k: len(v) for k, v in present.items()}
        print(f"{date_str}: {counts}")

        record = {"date": date_str}
        for key, _, _, _ in DATA_TYPES:
            # 取得失敗は null、データ無しは [] として残す。
            # 下流が「欠測」と「本当に0」を区別できるようにする。
            record[key] = payload[key]

        line = json.dumps({"raw_json": json.dumps(record, ensure_ascii=False),
                           "dt": date_str}, ensure_ascii=False) + "\n"
        try:
            save_json_to_s3(
                data=line,
                prefix=f"google_health/raw/dt={date_str}",
                file_name="data.jsonl",
            )
            saved += 1
        except Exception as e:
            print(f"{date_str} の保存に失敗: {e}")
            has_error = True

    print(f"保存した日数: {saved} / {len(buckets)}")

    try:
        create_external(system_name="google_health")
        sync_table_partition(table_name="google_health_external")
    except Exception as e:
        print(f"Trinoのメタデータ同期中にエラー: {e}")
        has_error = True

    if has_error:
        raise RuntimeError("一部の処理でエラーが発生した。ログを確認すること。")


if __name__ == "__main__":
    google_health_flow()
