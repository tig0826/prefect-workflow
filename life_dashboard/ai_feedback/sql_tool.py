"""LLM に渡す読み取り専用 SQL ツール。

なぜ必要か:
  固定 JSON を渡してコメントさせる形では、渡した範囲しか見られない。
  「予測できない問題」を拾うには LLM 自身に掘らせる必要がある
  （実際、チャット側は code_execution を持っていて日次FBより鋭い出力を出していた）。

ここで守ること:
  1. 読み取り専用   … SELECT / WITH 以外を拒否
  2. プライバシー境界 … URL・ページタイトル・曲名などの生値カラムを拒否
  3. 暴走の防止     … LIMIT 強制、行数上限、呼び出し回数上限
"""

import re

from common.trino_api import TrinoAPI

TRINO = TrinoAPI(host="trino.mynet", port=80, user="ai-feedback", catalog="iceberg")

MAX_ROWS = 200
MAX_CHARS = 12000

# 先頭がこれ以外なら拒否。CTE の後に INSERT を隠す手も封じたいので
# 危険キーワードの単語境界チェックも併用する。
_ALLOWED_START = re.compile(r"^\s*(select|with)\b", re.IGNORECASE)
_FORBIDDEN = re.compile(
    r"\b(insert|update|delete|merge|create|drop|alter|truncate|grant|revoke|call|"
    r"analyze|comment|refresh|set\s+session)\b",
    re.IGNORECASE,
)

# ★プライバシー境界★
# これらのカラムは silver に生値が残っているが、LLM のコンテキストには入れない。
# 分析に必要なのは「深夜にスマホで◯分ブラウジングしていた」という事実までで、
# 中身の識別は不要。本人が掘りたいときは自分で Trino を叩けばよい。
_FORBIDDEN_COLUMNS = re.compile(
    r"\b(url|page_title|media_title|media_artist|media_album|raw_window_title|"
    r"messages_json|breakfast_items|lunch_items|dinner_items|snack_items)\b",
    re.IGNORECASE,
)

# ドメイン名そのものを引かせない。
#
# 2026-08-27 の実害: cat_sub='プライベート' と life_silver.aw_web_events は塞いでいたが
# `domain` カラムを塞いでいなかったため、LLM が int_aw_web から上位ドメインを引き、
# 閲覧内容を名指しした insight をダッシュボードに書き込んだ。
# 「分類だけを上位層に出す」という設計意図に対して穴が空いていた。
#
# FB に必要なのは「深夜にスマホで◯分ブラウジングしていた」「逃避先が移動した」までで、
# 移動先の実体の識別は不要。移動そのものは cat_main / cat_sub の増減で言える。
_FORBIDDEN_PATTERNS = re.compile(
    r"(プライベート|life_silver\.aw_web_events|\bdomain\b)", re.IGNORECASE
)


class SqlToolError(Exception):
    pass


def validate(query: str) -> None:
    q = query.strip().rstrip(";")
    if not _ALLOWED_START.match(q):
        raise SqlToolError("SELECT か WITH で始まるクエリだけ実行できます（読み取り専用）")
    if _FORBIDDEN.search(q):
        raise SqlToolError("データを変更する構文は実行できません（読み取り専用）")
    if _FORBIDDEN_COLUMNS.search(q):
        raise SqlToolError(
            "URL・ページタイトル・曲名・食事の品目など、生の内容カラムは参照できません。"
            "分類（cat_main / cat_sub）と集計値を使ってください。"
        )
    if _FORBIDDEN_PATTERNS.search(q):
        raise SqlToolError(
            "ドメイン名・特定カテゴリの内訳・life_silver.aw_web_events は参照できません。"
            "閲覧先が移動したことは cat_main / cat_sub の増減で言えます。"
            "移動先の実体を名指しする必要はありません。"
        )
    if ";" in q:
        raise SqlToolError("複文は実行できません")


def run_sql(query: str) -> str:
    """検証してから実行し、Markdown 表の文字列で返す。"""
    validate(query)
    q = query.strip().rstrip(";")
    if not re.search(r"\blimit\s+\d+\s*$", q, re.IGNORECASE):
        q = f"{q}\nLIMIT {MAX_ROWS}"
    try:
        df = TRINO.execute_query(q)
    except Exception as e:  # noqa: BLE001
        return f"ERROR: {str(e)[:400]}"
    if df.empty:
        return "（0行）"
    if len(df) > MAX_ROWS:
        df = df.head(MAX_ROWS)
    text = df.to_markdown(index=False)
    if len(text) > MAX_CHARS:
        text = text[:MAX_CHARS] + f"\n…（{len(df)}行のうち先頭のみ表示）"
    return text


# LLM に渡すスキーマ説明。ここに書いていないテーブルは基本的に見なくてよい。
SCHEMA_DOC = """\
利用できるテーブル（すべて catalog=iceberg、読み取り専用）:

## life_gold.mrt_ai_activity_hourly ★行動分析の主テーブル
日 × 時 × ソース × ホスト × cat_main × cat_sub の秒数。**priority 抑制なし**なので
同じ時間に複数行が立つのが正しい（睡眠と画面が重なる = 寝落ち視聴が見える）。
  activity_date_jst DATE, hour_jst INT, source VARCHAR, hostname VARCHAR,
  cat_main VARCHAR, cat_sub VARCHAR, seconds BIGINT, minutes DOUBLE, event_count BIGINT
source は 'window'(前面アプリ) / 'media'(実再生) / 'web'(スマホブラウザ) / 'sleep'。
**source をまたぐと時間が重複するので合算しないこと。** 用途ごとに1つ選ぶ。

## life_gold.mrt_ai_screen_hourly ★「画面時間の総量」を出すときは必ずこれ
分単位で重複排除済みの画面時間。端末やソースをまたいでも同じ1分は1分。
  activity_date_jst DATE, hour_jst INT, screen_minutes BIGINT,
  top_cat_main VARCHAR, top_cat_minutes BIGINT
**mrt_ai_activity_hourly を source 横断で合算してはいけない**（同じ行動が二重に数えられる。
実測で 0-4時の画面時間が 258.4分/日 と出たが、正しくは 222.9分/日 = 16%過大だった）。
総量はこのテーブル、内訳や端末別は mrt_ai_activity_hourly。

## life_gold.mrt_ai_phone_daily
  target_date DATE, unlock_count INT, unlock_count_00_04 INT, unlock_count_09_18 INT,
  unlock_count_22_23 INT, longest_no_unlock_gap_min INT, rapid_reunlock_count INT
解錠回数は注意の断片化の直接指標。本人が自分では数えられない数字。

## life_gold.mrt_fitness_daily_summary
  target_date DATE, steps INT, total_minutes_asleep INT, total_time_in_bed INT,
  sleep_deep_minutes INT, sleep_rem_minutes INT, sleep_light_minutes INT,
  resting_heart_rate INT, weight_kg DOUBLE, body_fat_pct DOUBLE, bmi DOUBLE,
  sedentary_minutes INT, lightly_active_minutes INT, fairly_active_minutes INT,
  very_active_minutes INT, calories_in DOUBLE, calories_out INT,
  net_calorie_balance DOUBLE, weight_7d_avg DOUBLE, resting_hr_7d_avg DOUBLE
注意: 睡眠時間は Fitbit v1 由来で restless を睡眠に含むため約20%多め。

## life_gold.int_fitbit_sleep
  event_date_jst DATE（**セッション開始日**）, cat_sub VARCHAR('主睡眠'|'昼寝'),
  start_ts TIMESTAMP, end_ts TIMESTAMP
「昨夜の睡眠」は end_ts で絞ること。開始日基準なので深夜就寝だと翌日側に入る。

## life_silver.fitbit_sleep_stages ★睡眠段階の区間時系列（Fitbit v1.2）
  stage_date_jst DATE, log_id BIGINT, is_main_sleep BOOLEAN, is_short_wake BOOLEAN,
  stage_start_jst TIMESTAMP, stage_end_jst TIMESTAMP, stage_seconds INT,
  stage VARCHAR('deep'|'light'|'rem'|'wake')
覚醒区間の**時刻**が分かるので、解錠（life_silver.aw_unlock_events.unlock_ts_jst）や
画面時間（int_aw_media / int_aw_web）と時間重なりで突き合わせられる。
注意:
- v1 由来の `mrt_fitness_daily_summary.total_minutes_asleep` は restless を睡眠に含むため
  **v1.2 より 14.9% 多い**（23セッションで 4585分 vs 3991分）。混ぜて比較しないこと
- **v1 と v1.2 は同一セッションに別の log_id を振る**ので log_id で結合できない。
  突き合わせは時間の重なり（`a.start < b.end AND a.end > b.start`）で行う
- 日次集計するときは日跨ぎに注意（区間の開始日で分かれるため、セッション単位で見たいなら
  log_id で GROUP BY する）

## life_silver.fitbit_recovery（回復指標）
  target_date DATE, hrv_daily_rmssd DOUBLE, hrv_deep_rmssd DOUBLE, breathing_rate DOUBLE,
  spo2_avg/min/max DOUBLE, skin_temp_relative DOUBLE, vo2_max_range VARCHAR,
  azm_total/fat_burn/cardio/peak INT, signals_available INT
**前4指標は睡眠中計測でカバレッジ55%前後、しかも欠測日が完全に一致する MNAR**
（寝ていない夜・時計を外した夜にまとめて落ちる＝調子が悪い日に限って欠ける）。
NULL を 0 埋めしたり「改善」と読まないこと。`signals_available >= 3` の日だけ
複合として評価する。VO2max は文字列レンジで日次の状態には使えない。

## life_silver.aw_unlock_events（解錠の生タイムスタンプ）
  unlock_ts_jst TIMESTAMP, event_date_jst DATE, unlock_hour_jst INT
睡眠段階と突き合わせる用。**既知の検証結果（2026-08-27）**: 深夜0-5時の解錠115回のうち
主睡眠中に起きているのは8回のみで93%は入眠前。159の覚醒区間のうち解錠を伴ったのは8区間だけ。
つまりスマホは「睡眠を中断している」のではなく「入眠を遅らせている」。この点を誤らないこと。

## life_gold.mrt_aw_daily_work_summary
  target_date DATE, work_core_sec BIGINT, work_score INT, work_focus_rate INT,
  dev_core_sec BIGINT, dev_score INT, dev_focus_rate INT, work_apps_str VARCHAR,
  dev_apps_str VARCHAR

## life_gold.mrt_asken（食事・栄養）
  target_date DATE, calories_kcal DOUBLE, protein_g DOUBLE, fat_g DOUBLE,
  carbs_g DOUBLE, fiber_g DOUBLE, salt_g DOUBLE,
  breakfast_calories DOUBLE, lunch_calories DOUBLE, dinner_calories DOUBLE, snack_calories DOUBLE
（品目名のカラムは参照できません）

## life_gold.int_aw_media（実際に再生していた時間）
  event_date_jst DATE, start_ts, end_ts, duration_sec, cat_main, cat_sub, app_name
YouTube Music を YouTube から分離済み。前面時間とは別物。

## life_gold.int_aw_web（スマホブラウザの分類）
  event_date_jst DATE, start_ts, end_ts, duration_sec, domain, cat_main, cat_sub

## life_gold.int_aw_categorized（前面アプリの生イベント）
  event_date_jst DATE, start_ts, end_ts, is_afk BOOLEAN, cat_main, cat_sub, hostname
**必ず NOT is_afk で絞ること**（放置ウィンドウが混ざり数字が数倍になる）。

## life_gold.ai_feedback_issues / ai_interventions / ai_metric_history
課題・介入・metric 履歴。介入日で期間を切って前後比較する。

## life_gold.ai_feedback
  feedback_date DATE, slot VARCHAR, messages VARCHAR(JSON), generated_at TIMESTAMP
過去に何を言ったか。同じことを繰り返さないための参照。

制約:
- SELECT / WITH のみ。1文だけ。LIMIT は自動で付く（最大200行）
- URL・ページタイトル・曲名・食事の品目は参照できない
"""
