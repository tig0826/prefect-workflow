"""AW の bronze 欠損を自己修復するフロー。

なぜ必要か:
  aw_flow は毎回「昨日 + 今日」しか取得しないため、一度空いた穴が永久に埋まらない。
  実際に 2026-08-21〜08-24 の Android 系4バケット（test / unlock / media / web-chrome）が
  欠損していた。原因は2種類あり、どちらも aw_flow だけでは回収できない:

    1. クラスタ / Prefect / MinIO の障害でその日の書き込みが落ちた
    2. スマホがオフラインで、AW サーバへの同期が数日遅れた
       （遅れて届いた過去日のイベントは、その日を再取得しない限り bronze に入らない）

  AW サーバは全履歴を保持しているので、「AW にあって bronze に無い分」を
  日次で突き合わせて埋め直せば、両方の原因が自動で回収される。

やること:
  直近 lookback_days について、バケット × 日 の粒度で
  AW のイベント数と bronze のイベント数を比較し、差があるパーティションだけ
  再取得して上書きする。bronze の方が多い場合は AW 側の剪定とみなし、
  データを消さずに警告だけ出す（bronze を正とする）。
"""

from datetime import date, datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

from prefect import flow, task

from aw_exporter.aw_flow import (
    REGISTERED_BUCKETS,
    bronze_prefix,
    bucket_to_table_name,
    events_to_jsonl,
    scrape_aw_data,
)
from common.storage_tasks import save_json_to_s3
from common.trino_api import TrinoAPI
from common.trino_tasks import sync_table_partition

JST = ZoneInfo("Asia/Tokyo")
BRONZE_SCHEMA = "life_bronze"


@task(name="Count bronze events per bucket/day", retries=2, retry_delay_seconds=30)
def fetch_bronze_counts(start: date, end: date) -> dict[tuple[str, str], int]:
    """bronze の (バケット, dt) -> 件数 を1クエリでまとめて取る。

    未登録・未作成のテーブルは黙って飛ばす（初回実行時など）。
    """
    api = TrinoAPI(host="trino.mynet", port=80, user="tig", catalog="hive")
    existing = set()
    for bucket in REGISTERED_BUCKETS:
        table = bucket_to_table_name(bucket)
        if api.table_exists(table, BRONZE_SCHEMA):
            existing.add((bucket, table))
        else:
            print(f"⚠️  {BRONZE_SCHEMA}.{table} が未作成のためスキップ（aw_flow が先に作る）")

    if not existing:
        return {}

    start_str = start.strftime("%Y-%m-%d")
    end_str = end.strftime("%Y-%m-%d")
    unions = [
        f"SELECT '{bucket}' AS bucket, dt, COUNT(*) AS n "
        f"FROM {BRONZE_SCHEMA}.{table} "
        f"WHERE dt BETWEEN '{start_str}' AND '{end_str}' GROUP BY dt"
        for bucket, table in sorted(existing)
    ]
    df = api.execute_query("\nUNION ALL\n".join(unions))
    # 列名は必ず添字で引く。`row.dt` は pandas の .dt アクセサに食われて
    # AttributeError になる（列名 dt との衝突）。
    return {
        (str(r["bucket"]), str(r["dt"])): int(r["n"]) for _, r in df.iterrows()
    }


@flow(name="Life Metrics: ActivityWatch Reconcile", log_prints=True)
def aw_reconcile_flow(lookback_days: int = 30, dry_run: bool = False):
    """AW と bronze を突き合わせ、欠損パーティションを埋め直す。

    lookback_days: 何日分さかのぼって突き合わせるか
    dry_run: True なら差分を報告するだけで書き込まない
    """
    today = datetime.now(JST).date()
    # 今日はまだ増え続けるので突き合わせ対象から外す（aw_flow が15分ごとに更新している）
    end = today - timedelta(days=1)
    start = today - timedelta(days=lookback_days)
    print(f"🔍 突き合わせ対象: {start} 〜 {end} ({lookback_days}日, 今日は除外)")

    bronze_counts = fetch_bronze_counts(start, end)
    print(f"   bronze 側に {len(bronze_counts)} パーティション")

    filled: list[tuple[str, str, int, int]] = []   # bucket, dt, bronze件数, AW件数
    shrunk: list[tuple[str, str, int, int]] = []   # bronze の方が多かったもの
    touched_buckets: set[str] = set()
    failed_days: list[date] = []

    for offset in range((end - start).days + 1):
        d = start + timedelta(days=offset)
        dt_str = d.strftime("%Y-%m-%d")
        try:
            aw_data = scrape_aw_data(d)
        except Exception as e:
            print(f"❌ {dt_str} の AW 取得に失敗: {e}")
            failed_days.append(d)
            continue

        for bucket in REGISTERED_BUCKETS:
            aw_n = len(aw_data.get(bucket) or [])
            bronze_n = bronze_counts.get((bucket, dt_str), 0)
            if aw_n == bronze_n:
                continue
            if aw_n < bronze_n:
                # AW 側が少ない = AW の剪定か再構築。bronze を正として消さない。
                shrunk.append((bucket, dt_str, bronze_n, aw_n))
                continue

            print(f"🩹 {dt_str} [{bucket}] bronze={bronze_n} < aw={aw_n} → 埋め直し")
            filled.append((bucket, dt_str, bronze_n, aw_n))
            touched_buckets.add(bucket)
            if not dry_run:
                save_json_to_s3(
                    data=events_to_jsonl(aw_data[bucket]),
                    prefix=bronze_prefix(bucket, d),
                    file_name="data.jsonl",
                )

    # 新規パーティションを作った場合、sync しないと Trino から見えない
    if touched_buckets and not dry_run:
        for bucket in sorted(touched_buckets):
            sync_table_partition(table_name=bucket_to_table_name(bucket))

    print("\n" + "=" * 60)
    if filled:
        recovered = sum(aw_n - bronze_n for _, _, bronze_n, aw_n in filled)
        print(f"✅ {len(filled)} パーティションを埋め直し（+{recovered} イベント回収）")
        for bucket, dt_str, bronze_n, aw_n in filled:
            print(f"   {dt_str} [{bucket}] {bronze_n} -> {aw_n}")
    else:
        print("✅ 欠損なし")
    if shrunk:
        # 消さずに報告だけ。恒常的に出るなら AW 側の retention 設定を疑う。
        print(f"⚠️  AW の方が少ないパーティションが {len(shrunk)} 件（bronze を保持）")
        for bucket, dt_str, bronze_n, aw_n in shrunk:
            print(f"   {dt_str} [{bucket}] bronze={bronze_n} > aw={aw_n}")
    if dry_run:
        print("（dry_run のため書き込みはしていない）")

    if failed_days:
        # 一部の日が取れなくても他は直したいので、最後にまとめて失敗させる
        raise RuntimeError(
            f"{len(failed_days)}日分の AW 取得に失敗: "
            f"{[d.strftime('%Y-%m-%d') for d in failed_days]}"
        )

    return {
        "filled": len(filled),
        "recovered_events": sum(aw_n - b for _, _, b, aw_n in filled),
        "shrunk": len(shrunk),
    }


if __name__ == "__main__":
    # ローカルテスト用
    aw_reconcile_flow(lookback_days=30, dry_run=True)
