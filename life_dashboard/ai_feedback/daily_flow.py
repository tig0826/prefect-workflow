"""日次 AI フィードバック（起床トリガー・1日1回）。

従来との違い:
  旧: cron で朝8時/昼13時/夜22時の3回。毎回ゼロから同じデータを見てコメント。
      → 直近30日70件で「深夜」言及 81%、「概日リズム」49%。3枠が同じネタを再放送。
  新: 起床を検知して1日1回。できることを「進行中 issue の実況」と
      「新しい issue の提起（metric 必須）」に構造的に限定する。

この flow が担保すること:
  1. 冪等性        … 同じ日に二重生成しない
  2. metric 評価   … 全 active issue の metric_sql を実行して履歴に積む
  3. 仮説の棄却    … 言及回数が上限を超え metric が動かない issue を自動 abandon
  4. モード切替    … 体が壊れている日に最適化助言を出さない（安定化モード）
  5. 自明性フィルタ … 2段生成で「本人が既に知っていること」を捨てる

UI 互換のため slot は 'morning' のまま保存する（page.tsx が朝/昼/夜をハードコード
しているため、flow を先に出しても壊れない）。
"""

import datetime
import json
from zoneinfo import ZoneInfo

from google import genai
from google.genai import types
from prefect import flow, task
from prefect.blocks.system import Secret

from ai_feedback import issue_tracker as it
from ai_feedback.ai_feedback_flow import fetch_context
from common.trino_api import TrinoAPI

JST = ZoneInfo("Asia/Tokyo")
TRINO = TrinoAPI(host="trino.mynet", port=80, user="tig", catalog="iceberg")

MODEL = "gemini-pro-latest"
# 6KB のコンテキストに対して旧設定の 1024 は薄すぎた。因果推論をさせるので厚くする。
THINKING_BUDGET = 8192

SLOT = "morning"  # UI 互換のため固定

# ── 安定化モードの閾値 ────────────────────────────────────────
# 過去30日で検証した結果に基づく（発火 5/26日 = 19%）。
# 分布実測(n=70): 睡眠 p10=2.5h / p25=4.3h / p50=6.4h、摂取 p10=712 / p25=1341 kcal。
#
# 安静時心拍は基準から外した。理由が2つある:
#   ① n=27 の検定で睡眠時間・深睡眠・work_score のいずれとも有意な相関がなかった
#      （既存FBの「安静時心拍3日連続上昇→疲労蓄積」は未検証の主張だった）
#   ② resting_hr_7d_avg が RHR=0 の欠測日に汚染され 68.6〜76 の間で跳ねていて、
#      「平均からの乖離」が信頼できない
#
# 注意: 閾値は同じ30日で選んで同じ30日で検証している（ホールドアウトなし）。
# 個人用システムとしては妥当な割り切りだが、統計的に検証済みではない。
STABILIZE_SLEEP_H = 3.0            # これ未満なら単独で安定化
STABILIZE_SLEEP_H_WITH_LOW_CAL = 4.5
STABILIZE_LOW_CAL = 900.0
# 摂取カロリー単独では判定しない。あすけんの記録漏れと本当の低摂取が区別できず、
# 実際 8/19 は睡眠10.8hで摂取419kcal（記録漏れ）だったため。


# ─────────────────────────────────────────────────────────────
# 起床判定
# ─────────────────────────────────────────────────────────────
# 「起床した」と見なす睡眠セッションの最小長。これ未満は仮眠・寝返り扱い。
MIN_WAKE_SESSION_MIN = 60


@task(name="Detect wake-up", retries=2, retry_delay_seconds=30)
def detect_wake(target_date: str) -> dict:
    """今朝までに終わった睡眠セッションから起床を検知する。

    ★`cat_sub='主睡眠'` で絞ってはいけない★
    主睡眠/昼寝は Fitbit の isMainSleep をそのまま使っており、Fitbit は
    **その日の最長セッションを主睡眠**とする。昼寝が夜間睡眠より長いと逆転する。
    実測（2026-08-27）:
        15:18→20:03 (284分) = 主睡眠   ← 午後の睡眠
        03:46→06:02 (135分) = 昼寝     ← 実際の夜間睡眠
    この日に `cat_sub='主睡眠'` で探すと、朝の時点では該当が無く起床を検知できず、
    11時の強制発火まで待つことになる。

    そこでラベルに依存せず「一定時間以上のセッションが終わっていること」で判定する。
    gate は 05:00-11:00 しか回らないので、午後の昼寝で誤発火することはない。
    加えて生成済みなら gate 側で早期 return するため二重生成もしない。
    """
    df = TRINO.execute_query(f"""
        SELECT CAST(max(end_ts) AS VARCHAR) AS wake_ts,
               max(date_diff('minute', start_ts, end_ts)) AS dur_min,
               array_join(array_agg(DISTINCT cat_sub), ',') AS labels
        FROM iceberg.life_gold.int_fitbit_sleep
        WHERE end_ts >= TIMESTAMP '{target_date} 00:00:00'
          AND end_ts <  TIMESTAMP '{target_date} 12:00:00'
          AND date_diff('minute', start_ts, end_ts) >= {MIN_WAKE_SESSION_MIN}
    """)
    if df.empty or df.iloc[0, 0] is None:
        return {"woke": False, "wake_ts": None, "dur_min": None}
    r = it._records(df)[0]
    return {
        "woke": True,
        "wake_ts": str(r["wake_ts"]),
        "dur_min": int(r["dur_min"]) if r["dur_min"] is not None else None,
        "labels": r.get("labels"),
    }


@task(name="Check feedback already exists")
def already_generated(target_date: str) -> bool:
    df = TRINO.execute_query(f"""
        SELECT count(*) AS n FROM iceberg.life_gold.ai_feedback
        WHERE feedback_date = DATE '{target_date}' AND slot = '{SLOT}'
    """)
    return (not df.empty) and int(df.iloc[0]["n"]) > 0


# ─────────────────────────────────────────────────────────────
# コンテキスト構築
# ─────────────────────────────────────────────────────────────
@task(name="Fetch last night sleep")
def fetch_last_night(night_date: str) -> dict:
    """今朝までの睡眠（＝昨夜の睡眠）を取る。

    ★重要な日付の話★
    `int_fitbit_sleep.event_date_jst` は **セッションの開始日**。
    したがって「昨夜寝て今朝起きた」睡眠は、就寝が 0時前なら前日、
    0時を過ぎていれば今日の日付に入る。実測:
       8/25 22:08 → 8/26 03:03 (295分)  ... event_date = 8/25
       8/27 03:46 → 8/27 06:02 (135分)  ... event_date = 8/27

    旧実装は朝スロットで analysis_date = target_date - 1 として全データを引いていた。
    そのため深夜就寝の日は昨夜の睡眠を丸ごと取り逃し、前々夜を語っていた
    （8/27朝のFBが実際の2.1時間睡眠に触れず、8/26の昼寝を論じていたのがこれ）。

    ここでは「今日の朝までに終わった睡眠」を終了時刻基準で拾う。
    """
    df = TRINO.execute_query(f"""
        SELECT cat_sub,
               CAST(start_ts AS VARCHAR) AS start_ts,
               CAST(end_ts AS VARCHAR) AS end_ts,
               date_diff('minute', start_ts, end_ts) AS dur_min
        FROM iceberg.life_gold.int_fitbit_sleep
        WHERE end_ts >= TIMESTAMP '{night_date} 00:00:00'
          AND end_ts <  TIMESTAMP '{night_date} 12:00:00'
        ORDER BY start_ts
    """)
    if df.empty:
        return {"データなし": True, "note": "今朝までに終わった睡眠セッションが無い（徹夜・未装着・同期遅延のいずれか）"}

    sessions = it._records(df)
    main = [s for s in sessions if s["cat_sub"] == "主睡眠"]
    return {
        "セッション": [
            {"種別": s["cat_sub"], "就寝": s["start_ts"][:16], "起床": s["end_ts"][:16], "分": int(s["dur_min"])}
            for s in sessions
        ],
        "主睡眠_分": sum(int(s["dur_min"]) for s in main) or None,
        "主睡眠_就寝時刻": main[0]["start_ts"][11:16] if main else None,
        "主睡眠_起床時刻": main[-1]["end_ts"][11:16] if main else None,
        "note": "これが昨夜の睡眠。日中の活動データ（today）は前日のものなので混同しないこと。",
    }


@task(name="Fetch overnight phone signals")
def fetch_overnight_unlocks(night_date: str) -> dict:
    """昨夜（今日の0-6時）の解錠。睡眠中断の物証。

    mrt_ai_phone_daily は暦日集計なので、今朝の分は今日の日付を見る。
    """
    df = TRINO.execute_query(f"""
        SELECT count(*) AS n,
               count_if(unlock_hour_jst BETWEEN 0 AND 4) AS n_00_04
        FROM iceberg.life_silver.aw_unlock_events
        WHERE unlock_ts_jst >= TIMESTAMP '{night_date} 00:00:00'
          AND unlock_ts_jst <  TIMESTAMP '{night_date} 06:00:00'
    """)
    if df.empty:
        return {}
    return {
        "今朝0-6時の解錠回数": int(df.iloc[0]["n"]),
        "うち0-4時": int(df.iloc[0]["n_00_04"]),
    }


@task(name="Fetch last night sleep stages")
def fetch_last_night_stages(night_date: str) -> dict:
    """昨夜の睡眠段階（Fitbit v1.2）と、覚醒区間中の解錠。

    v1 の合計時間より信頼できる（v1 は restless を睡眠に含むため 14.9% 多い）。
    覚醒区間の時刻が分かるので「起きた瞬間にスマホを開いたか」を判定できる。
    """
    df = TRINO.execute_query(f"""
        WITH s AS (
            SELECT stage, is_short_wake, stage_start_jst, stage_end_jst, stage_seconds
            FROM iceberg.life_silver.fitbit_sleep_stages
            WHERE is_main_sleep
              AND stage_end_jst >= TIMESTAMP '{night_date} 00:00:00'
              AND stage_end_jst <  TIMESTAMP '{night_date} 12:00:00'
        )
        SELECT
            round(sum(CASE WHEN stage = 'deep'  THEN stage_seconds END)/60.0, 0) AS deep_min,
            round(sum(CASE WHEN stage = 'light' THEN stage_seconds END)/60.0, 0) AS light_min,
            round(sum(CASE WHEN stage = 'rem'   THEN stage_seconds END)/60.0, 0) AS rem_min,
            round(sum(CASE WHEN stage = 'wake' OR is_short_wake THEN stage_seconds END)/60.0, 0) AS wake_min,
            count_if(stage = 'wake' OR is_short_wake) AS wake_events,
            (SELECT count(*) FROM s w
              JOIN iceberg.life_silver.aw_unlock_events u
                ON u.unlock_ts_jst >= w.stage_start_jst AND u.unlock_ts_jst < w.stage_end_jst
             WHERE w.stage = 'wake' OR w.is_short_wake) AS unlocks_during_wake
        FROM s
    """)
    if df.empty:
        return {}
    r = it._records(df)[0]
    # count_if は行が無くても 0 を返すので、0 だけでは「データなし」と区別できない。
    # 段階の分数が全て NULL なら中身が無い＝渡さない（全NULLの辞書を渡すと
    # 「睡眠段階のデータがありません」と言い出す原因になる。存在しないデータへの
    # 言及はプロンプトで禁止しているが、そもそも渡さないのが確実）。
    if all(r.get(k) is None for k in ("deep_min", "light_min", "rem_min", "wake_min")):
        return {}
    return {
        "深い睡眠_分": r["deep_min"],
        "浅い睡眠_分": r["light_min"],
        "REM_分": r["rem_min"],
        "覚醒_分": r["wake_min"],
        "覚醒回数": r["wake_events"],
        "覚醒中に解錠した回数": r["unlocks_during_wake"],
        "note": (
            "Fitbit v1.2 の段階データ。today.sleep_deep_min（v1由来）とは基準が違うので混ぜないこと。"
            "既知の検証結果: 深夜の解錠は93%が入眠前で、睡眠中の覚醒とはほぼ無関係。"
            "『覚醒中に解錠した回数』が0でも異常ではない。"
        ),
    }


@task(name="Fetch recovery signals")
def fetch_recovery(night_date: str) -> dict:
    """HRV・呼吸数・SpO2・皮膚温。**3つ以上揃った日だけ渡す。**

    4指標はすべて睡眠中計測で、欠測日が完全に一致する MNAR（悪い夜に限って落ちる）。
    1つだけ取れた日の値を根拠に語らせると、欠測バイアスをそのまま増幅する。
    """
    df = TRINO.execute_query(f"""
        SELECT hrv_daily_rmssd, hrv_deep_rmssd, breathing_rate, spo2_avg,
               skin_temp_relative, signals_available
        FROM iceberg.life_silver.fitbit_recovery
        WHERE target_date = DATE '{night_date}'
    """)
    if df.empty:
        return {}
    r = it._records(df)[0]
    if (r.get("signals_available") or 0) < 3:
        return {}

    # 直近28日の中位数と比べる。個人の絶対値には意味が薄い指標なので相対で見る。
    base = TRINO.execute_query(f"""
        SELECT round(approx_percentile(hrv_daily_rmssd, 0.5), 1) AS hrv_med,
               round(approx_percentile(breathing_rate, 0.5), 1) AS br_med,
               round(approx_percentile(spo2_avg, 0.5), 1) AS spo2_med,
               round(approx_percentile(skin_temp_relative, 0.5), 1) AS temp_med
        FROM iceberg.life_silver.fitbit_recovery
        WHERE target_date BETWEEN DATE '{night_date}' - INTERVAL '28' DAY
                              AND DATE '{night_date}' - INTERVAL '1' DAY
          AND signals_available >= 3
    """)
    b = it._records(base)[0] if not base.empty else {}
    return {
        "HRV": r["hrv_daily_rmssd"], "HRV_28日中位数": b.get("hrv_med"),
        "呼吸数": r["breathing_rate"], "呼吸数_28日中位数": b.get("br_med"),
        "SpO2": r["spo2_avg"], "SpO2_28日中位数": b.get("spo2_med"),
        "皮膚温相対": r["skin_temp_relative"], "皮膚温_28日中位数": b.get("temp_med"),
        "取得できた指標数": r["signals_available"],
        "note": (
            "4指標すべて睡眠中の計測で、欠測日が一致する（悪い夜に落ちる）。"
            "3つ以上揃った日だけ渡している。**単独の指標では語らず、"
            "複数が同じ方向に中位数から外れている場合にだけ言及すること。**"
            "絶対値ではなく中位数からの乖離で見る。"
        ),
    }


@task(name="Fetch awake span activity")
def fetch_awake_span(span_start: str, span_end: str) -> dict:
    """「前日の朝〜今朝の起床まで」を**1つの連続した区間**として活動を拾う。

    ★なぜ暦日ではダメか★
    暦日で切ると、前日の夜〜深夜（22時〜翌03時など）が
    「前日の集計」と「昨夜の睡眠」の隙間に落ちる。本人の体験としては
    「起きてから次に起きるまで」が1サイクルなので、その区間で見るのが自然。

    span_start … 前日の起床時刻（取れなければ前日06:00）
    span_end   … 今朝の起床時刻
    """
    df = TRINO.execute_query(f"""
        WITH ev AS (
            SELECT 'screen' AS src, cat_main, cat_sub, start_ts, end_ts
            FROM iceberg.life_gold.int_aw_categorized
            WHERE NOT is_afk
              AND start_ts < TIMESTAMP '{span_end}' AND end_ts > TIMESTAMP '{span_start}'
            UNION ALL
            SELECT 'media', cat_main, cat_sub, start_ts, end_ts
            FROM iceberg.life_gold.int_aw_media
            WHERE start_ts < TIMESTAMP '{span_end}' AND end_ts > TIMESTAMP '{span_start}'
            UNION ALL
            SELECT 'sleep', cat_main, cat_sub, start_ts, end_ts
            FROM iceberg.life_gold.int_fitbit_sleep
            WHERE start_ts < TIMESTAMP '{span_end}' AND end_ts > TIMESTAMP '{span_start}'
        )
        SELECT src, cat_main, cat_sub,
               round(sum(date_diff('second',
                   GREATEST(start_ts, TIMESTAMP '{span_start}'),
                   LEAST(end_ts, TIMESTAMP '{span_end}')))/60.0, 0) AS minutes
        FROM ev
        GROUP BY 1, 2, 3
        HAVING sum(date_diff('second',
                   GREATEST(start_ts, TIMESTAMP '{span_start}'),
                   LEAST(end_ts, TIMESTAMP '{span_end}'))) >= 300
        ORDER BY 4 DESC
    """)
    rows = it._records(df)
    if not rows:
        return {}
    by_src: dict = {}
    for r in rows:
        by_src.setdefault(r["src"], {})[r["cat_sub"]] = r["minutes"]

    # 時間帯別の画面時間（重複排除済み）も同じ区間で出す
    hourly = TRINO.execute_query(f"""
        SELECT hour_jst, sum(screen_minutes) AS m
        FROM iceberg.life_gold.mrt_ai_screen_hourly
        WHERE activity_date_jst BETWEEN CAST(TIMESTAMP '{span_start}' AS DATE)
                                    AND CAST(TIMESTAMP '{span_end}' AS DATE)
        GROUP BY 1 ORDER BY 1
    """)
    return {
        "区間": f"{span_start} 〜 {span_end}（前回の起床から今朝の起床まで）",
        "分_source別cat_sub別": by_src,
        "画面時間_時間帯別": {str(r["hour_jst"]): r["m"] for r in it._records(hourly)},
        "note": (
            "これが今回のFBの主対象。前日の朝から今朝の起床までを1サイクルとして見ている。"
            "暦日で切ると前日の夜〜深夜が集計の隙間に落ちるため、この区間で語ること。"
            "source をまたぐと時間は重複するので合算しない。"
        ),
    }


@task(name="Detect previous wake")
def detect_previous_wake(span_end: str) -> str:
    """前回の起床時刻（＝区間の始まり）を返す。取れなければ前日06:00。"""
    df = TRINO.execute_query(f"""
        SELECT CAST(max(end_ts) AS VARCHAR) AS prev_wake
        FROM iceberg.life_gold.int_fitbit_sleep
        WHERE end_ts < TIMESTAMP '{span_end}'
          AND end_ts >= TIMESTAMP '{span_end}' - INTERVAL '36' HOUR
          AND date_diff('minute', start_ts, end_ts) >= {MIN_WAKE_SESSION_MIN}
    """)
    if not df.empty and df.iloc[0, 0] is not None:
        return str(df.iloc[0]["prev_wake"])[:19]
    fallback = (
        datetime.datetime.fromisoformat(span_end) - datetime.timedelta(days=1)
    ).replace(hour=6, minute=0, second=0, microsecond=0)
    return fallback.strftime("%Y-%m-%d %H:%M:%S")


@task(name="Fetch phone signals")
def fetch_phone_signals(analysis_date: str) -> dict:
    """解錠回数から注意の断片化を取る。本人が自分では数えられない数字。"""
    df = TRINO.execute_query(f"""
        SELECT unlock_count, unlock_count_00_04, unlock_count_09_18,
               unlock_count_22_23, longest_no_unlock_gap_min, rapid_reunlock_count
        FROM iceberg.life_gold.mrt_ai_phone_daily
        WHERE target_date = DATE '{analysis_date}'
    """)
    if df.empty:
        return {}
    r = df.iloc[0]
    return {
        "解錠回数": int(r["unlock_count"]),
        "深夜0-4時の解錠": int(r["unlock_count_00_04"]),
        "就業9-18時の解錠": int(r["unlock_count_09_18"]),
        "夜22-23時の解錠": int(r["unlock_count_22_23"]),
        "最長無操作区間_分": int(r["longest_no_unlock_gap_min"]) if r["longest_no_unlock_gap_min"] is not None else None,
        "5分以内の連続解錠": int(r["rapid_reunlock_count"]),
        "note": "解錠回数は注意の断片化の直接指標。深夜帯は睡眠中断の物証になる。",
    }


@task(name="Fetch cat_sub level activity")
def fetch_activity_detail(analysis_date: str) -> dict:
    """cat_sub 粒度・priority 抑制なしの活動。睡眠と画面の重なりが見える。

    source をまたぐと重複するので、用途ごとに分けて渡す。
    """
    df = TRINO.execute_query(f"""
        SELECT source, cat_main, cat_sub, hour_jst, seconds
        FROM iceberg.life_gold.mrt_ai_activity_hourly
        WHERE activity_date_jst = DATE '{analysis_date}'
        ORDER BY hour_jst
    """)
    if df.empty:
        return {}

    by_source_cat: dict = {}
    overlap_sleep_screen: dict = {}
    sleep_by_hour: dict = {}
    screen_by_hour: dict = {}

    for _, r in df.iterrows():
        src, cm, cs, h, sec = r["source"], r["cat_main"], r["cat_sub"], int(r["hour_jst"]), int(r["seconds"])
        by_source_cat.setdefault(src, {}).setdefault(cs, 0)
        by_source_cat[src][cs] += round(sec / 60)
        if src == "sleep":
            sleep_by_hour[h] = sleep_by_hour.get(h, 0) + sec
        elif cm in ("MEDIA", "MANGA", "BROWSING", "GAME"):
            screen_by_hour[h] = screen_by_hour.get(h, 0) + sec

    # 同じ時間に睡眠と画面が両方立っている = 寝落ち視聴の候補。
    # 旧 mrt_behavior_slots_15m は priority で SLEEP が勝つため原理的に見えなかった。
    for h in sorted(set(sleep_by_hour) & set(screen_by_hour)):
        overlap_sleep_screen[str(h)] = {
            "睡眠_分": round(sleep_by_hour[h] / 60),
            "画面_分": round(screen_by_hour[h] / 60),
        }

    # 0分に丸まった項目は捨てる（ノイズをLLMに渡さない）
    cleaned = {
        src: {k: v for k, v in sorted(cats.items(), key=lambda x: -x[1]) if v > 0}
        for src, cats in by_source_cat.items()
    }
    out = {"分_source別cat_sub別": cleaned}
    if overlap_sleep_screen:
        out["睡眠と画面が重なった時間帯"] = overlap_sleep_screen
        out["note"] = (
            "睡眠と画面が同じ時間に立っているのは寝落ち視聴の候補。"
            "source をまたぐと時間は重複するので合算しないこと。"
        )
    return out


def detect_mode(ctx: dict) -> dict:
    """最適化モード / 安定化モードを判定する。

    体の基本状態が壊れている日に「仮眠は20分に抑えて」のような最適化助言を
    出すのは役に立たないだけでなく追い詰める方向に働くため、モードを分ける。

    睡眠は **昨夜（last_night）** を見る。前日の睡眠ではなく、今まさに
    寝不足で起きたかどうかが今日のモードを決めるため。
    """
    last_night = ctx.get("last_night") or {}
    main_min = last_night.get("主睡眠_分")
    sleep_h = round(main_min / 60, 1) if main_min else None
    # 前日の摂取。昨夜の睡眠と組み合わせて判定する。
    cal_in = ((ctx.get("today") or {}).get("meals") or {}).get("total_kcal")

    reasons = []
    # 睡眠データが無い日は判定しない（欠測を「良い」とも「悪い」とも読まない）
    if sleep_h is not None and sleep_h > 0:
        if sleep_h < STABILIZE_SLEEP_H:
            reasons.append(f"睡眠が {sleep_h}h（{STABILIZE_SLEEP_H}h 未満）")
        elif (
            sleep_h < STABILIZE_SLEEP_H_WITH_LOW_CAL
            and cal_in is not None and 0 < cal_in < STABILIZE_LOW_CAL
        ):
            reasons.append(
                f"睡眠 {sleep_h}h かつ摂取 {cal_in:.0f}kcal（両方が下位圏）"
            )

    return {
        "mode": "stabilize" if reasons else "optimize",
        "reasons": reasons,
    }


@task(name="Build daily context")
def build_daily_context(day_date: str, night_date: str, eval_date: str) -> dict:
    """2つの日付スコープを持つコンテキストを作る。

    day_date   … 総括する「昨日1日」の行動・作業・食事
    night_date … 「昨夜の睡眠」（= 今日の日付。int_fitbit_sleep は開始日基準なので
                  深夜就寝だと今日側に入る）

    旧実装は両方を target_date - 1 で引いていたため、深夜就寝の日は
    昨夜の睡眠を取り逃していた。
    """
    ctx = fetch_context(day_date)
    ctx["_date_scope"] = {
        "awake_span": "★主対象★ 前回の起床から今朝の起床までの連続区間。ここを中心に語る",
        "today": f"{day_date}（前日の暦日集計: 作業スコア・食事・歩数など日単位の指標）",
        "last_night": f"{night_date} の朝までに終わった睡眠（= 昨夜）",
        "note": (
            "**awake_span が本題**。前日の朝から今朝の起床までを1サイクルとして見る。"
            "today は暦日単位でしか出せない指標（work_score・摂取カロリー・歩数）のために残してあるが、"
            "行動の話は awake_span を使うこと（暦日で切ると前日の夜〜深夜が隙間に落ちる）。"
        ),
    }

    # ★今回のFBの主対象: 前回の起床から今朝の起床までの連続区間★
    wake = detect_wake(night_date)
    span_end = (wake.get("wake_ts") or f"{night_date} 09:00:00")[:19]
    span_start = detect_previous_wake(span_end)
    awake = fetch_awake_span(span_start, span_end)
    if awake:
        ctx["awake_span"] = awake

    ctx["last_night"] = fetch_last_night(night_date)
    overnight = fetch_overnight_unlocks(night_date)
    if overnight:
        ctx["last_night"]["解錠"] = overnight
    stages = fetch_last_night_stages(night_date)
    if stages:
        ctx["last_night"]["段階"] = stages
    recovery = fetch_recovery(night_date)
    if recovery:
        ctx["last_night"]["回復指標"] = recovery

    phone = fetch_phone_signals(day_date)
    if phone:
        ctx.setdefault("today", {})["phone_signals"] = phone

    detail = fetch_activity_detail(day_date)
    if detail:
        ctx.setdefault("today", {})["activity_detail"] = detail

    # 進行中の課題と、その metric が実際にどう動いたか
    evaluated = it.evaluate_all_issues(eval_date)
    abandoned = it.enforce_abandonment(evaluated)
    ctx["active_issues"] = [
        {
            "issue_id": e["issue_id"],
            "title": e["title"],
            "hypothesis": e["hypothesis"],
            "status": e["status"],
            "metric": e["metric_name"],
            "unit": e["metric_unit"],
            "baseline": e["baseline_value"],
            "current": e["current_value"],
            "target": e["target_value"],
            "direction": e["target_direction"],
            "change_pct": e["change_from_baseline_pct"],
            "progress_pct": e["progress_pct"],
            "is_moving": e["is_moving"],
            "mention_count": e["mention_count"],
            "eval_error": e["eval_error"],
            "history": [
                {"d": h["eval_date"], "v": h["metric_value"]}
                for h in (e.get("history") or []) if h.get("metric_value") is not None
            ][-14:],
        }
        for e in evaluated if e["status"] in ("open", "testing")
    ]
    if abandoned:
        ctx["abandoned_today"] = [
            {"issue_id": a["issue_id"], "title": a["title"], "reason": a.get("abandon_reason")}
            for a in abandoned
        ]

    ctx["interventions"] = [
        {
            "started": v["started_at"][:10] if v.get("started_at") else None,
            "ended": v["ended_at"][:10] if v.get("ended_at") else "継続中",
            "kind": v["kind"],
            "description": v["description"],
        }
        for v in it.load_interventions(limit_days=60)
    ]

    ctx["mode"] = detect_mode(ctx)
    return ctx


# ─────────────────────────────────────────────────────────────
# 生成（2段: 自明性フィルタ → 本生成）
# ─────────────────────────────────────────────────────────────
_OBVIOUS_PROMPT = """\
あなたはライフログの分析者です。以下のデータから「本人が自分で見て当然気づくこと」を
列挙してください。これは**除外リストを作るための作業**なので、価値判断や助言は不要です。

本人はその日を生きた当事者です。したがって以下は全て「自明」です:
- 数値をそのまま言い換えたもの（「睡眠が4時間でした」）
- 本人が意識的に行った行動（「深夜まで開発していた」「昼食を抜いた」）
- 単一指標の高低（「歩数が少ない」「摂取カロリーが低い」）
- 一般常識で予測できる因果（「寝不足だと集中しにくい」）

## データ
{data}

## 出力形式（JSONのみ）
{{"obvious": ["自明な観察1", "自明な観察2", ...]}}

10〜20件挙げてください。多いほど後段の質が上がります。"""


_MAIN_PROMPT_HEADER = """\
あなたはユーザー専属のライフアナリストです。1日1回、朝に届くフィードバックを書きます。

## このフィードバックの役割

**あなたの仕事は「洞察の配達」ではなく「進行中の実験の実況」です。**
生活習慣は日次では変化しません。日次で変わるのはノイズだけです。
毎日新しい洞察を出そうとすると、必ず同じ話の言い換えになります（実際、旧実装では
直近30日70件のうち81%が「深夜」に言及していました）。

読む価値を作るのは連続性です。本人が参加している実験のスコアが動いているから読む。

## 絶対の制約

### 1. 自明なことを書かない
`already_obvious` に、本人が自分で気づくことが列挙されています。
**これらと同じ内容・同じ角度のことは一切書いてはいけません。**
数値の言い換え、本人が意識してやった行動の指摘、単一指標の高低は全て禁止です。

### 2. 処方は metric を書けるものだけ
新しい提案をする場合、**既存テーブルのカラムと閾値で自動検証できる形**でなければ
出力してはいけません。
- ✗ 「十分な睡眠を」「無理は禁物」「仮眠は20分に」← 検証不能
- ○ 「今週は23時以降の U-NEXT を止める」← int_aw_categorized で毎朝自動検証できる
検証できない助言は、それ自体が無価値です。

### 3. 統計的に弱い主張をしない
- 相関の項目（`*_corr`）は有意性検定を通ったものだけが渡されています。
  渡っていない相関について推測で「傾向がある」と語ってはいけません。
- `confidence: "moderate"` は「偶然ではなさそう」程度です。断定しないでください。
- `kind: "descriptive"` は記述統計であり因果ではありません。
- n が小さい項目は言い切りを避けてください。

### 4. awake_span（前回の起床〜今朝の起床）が主対象
`awake_span` は「前回起きてから今朝起きるまで」の連続区間です。**ここを中心に語ってください。**
本人の体験としては「起きてから次に起きるまで」が1サイクルなので、
暦日で切ると前日の夜〜深夜が集計の隙間に落ちます。

`today` は暦日単位でしか出せない指標（work_score / 摂取カロリー / 歩数）のために
残してありますが、**行動の話は awake_span を使ってください。**

### 5. 日付スコープを混同しない
`_date_scope` を必ず確認してください。
- `today` は **昨日1日** の行動・作業・食事です
- `last_night` は **昨夜の睡眠** です（今朝起きた分）
「昨夜◯時間しか寝ていない」と言うときは必ず `last_night` を根拠にしてください。
`today.sleep_hours` は前日の睡眠なので、昨夜の話に使ってはいけません。

### 6. 因果の向きを問う
症状と原因を区別してください。介入すべきは原因側です。
例: 「昼寝が長い」は症状です。主睡眠が短いから昼寝で補填している場合、
昼寝を削ると総睡眠が減るだけで悪化します。

### dev_score / work_score の測定上の限界（誤読しやすい）

- **dev_h の増加を単独で「生産性向上」と読んではいけない。** 同じ日に work_h が
  落ちている場合、それは業務から個人開発への**置き換え**であって総生産量の増加ではない。
  実測（2026-08-27）: work 1.0h/score 1 に対し dev 6.6h/score 120。本人の申告は
  「仕事をサボって開発した。昼寝もして気分も落ちていた」。にもかかわらず
  週次FBは「日中の生産性に劇的な効果」と肯定的に評価していた。
  **work_h と dev_h は必ず同時に見て、合計と配分の両方を述べること。**
- **dev_score はアプリ前面時間ベースで、成果量ではない。** AIペアプロ（Gemini/Claude等）の
  待ち時間も計上される。「開発した」ことを能力や達成の指標として褒めない。
- 高い dev_score と 低い work_score が同時に出た日は、**回避行動の可能性**として扱い、
  睡眠・気分の指標と合わせて解釈する。

### 7. 禁止事項
- JSONキー名・変数名の出力
- 数値の羅列
- 存在しないデータへの言及（「〇〇のデータがありません」）
- 一般論（このユーザーのデータに根拠を置かない助言）
"""

_MODE_OPTIMIZE = """\
## 今日のモード: 最適化

構成は以下の順で組み立ててください。

1. **進行中の実験の実況（必須・最優先）**
   `active_issues` の各項目について、metric が baseline からどう動いたかを伝えます。
   - `is_moving: true` → 効いています。どれだけ動いたかを具体的に
   - `is_moving: false` かつ `mention_count` が多い → 効いていないことを正直に言う
   - `eval_error` がある → 評価できていない。**欠測を「改善」と読まないこと**
   - `history` があれば動きの方向（改善中/停滞/悪化）を見る

2. **今日固有の事実（`already_obvious` にないもの）**
   `activity_detail` の「睡眠と画面が重なった時間帯」、`phone_signals` の解錠回数など、
   **本人が自分では数えられない数字**を優先してください。

3. **処方（最大1つ・省略可）**
   制約2を満たせる場合のみ。満たせないなら書かないでください。

`abandoned_today` があれば、その仮説を諦めたことを1文で伝えてください
（同じことを言い続けないための機構が働いた、という報告です）。
"""

_MODE_STABILIZE = """\
## 今日のモード: 安定化

**体の基本状態が壊れています**（`mode.reasons` を参照）。

この状態で「仮眠は20分に抑えて」「概日リズムを整えて」のような最適化助言を出すのは、
役に立たないだけでなく追い詰める方向に働きます。**今日は書かないでください。**

代わりに:
- 目標は「今日を無事に終える」の1点だけです
- **danger を積み上げないこと。** 悪い数値を並べて危機感を煽らない
- 進行中の実験の実況も今日は不要です（スコアの話をする日ではありません）
- metric 必須のルールもこのモードでは外します（休息は metric で測るものではない）
- 具体的で負荷の小さい、体を回復させる行動を1つだけ
- 事実の指摘は最小限に。本人は既に自覚しています

件数は1〜2件に抑えてください。type は "positive" か "insight" を使い、
"danger" は使わないでください。
"""

_OUTPUT_RULES = """\
## 出力ルール
- 件数: 最適化モードは2〜3件、安定化モードは1〜2件
- 1件 = 150文字以内
- type: "positive" | "warning" | "danger" | "insight"
- **`issue_ids`: そのメッセージで扱った `active_issues` の issue_id を配列で必ず入れる**
  （どの課題にも紐づかない新規の指摘なら空配列 `[]`）
  これは言及回数の記録に使われ、同じ課題を言い続けた場合に仮説を棄却する
  機構の入力になります。省略すると機構が動かないので必ず入れてください。

## 出力形式（JSONのみ・他のテキスト一切不要）
[{"type": "insight", "message": "...", "issue_ids": ["ISS-XXXXXXXX"]}]
"""


def _extract_json(text: str):
    text = text.strip()
    if "```" in text:
        parts = text.split("```")
        text = parts[1] if len(parts) > 1 else parts[0]
        if text.startswith("json"):
            text = text[4:]
    return json.loads(text.strip())


@task(name="Generate daily feedback", retries=2, retry_delay_seconds=30)
def generate_daily(ctx: dict, api_key: str) -> tuple[list[dict], list[str]]:
    client = genai.Client(api_key=api_key)
    data_json = json.dumps(ctx, ensure_ascii=False, indent=2, default=str)

    # ── 1段目: 自明なことを列挙させる（出力はユーザーに見せない）──
    # 過去FBとの重複チェックでは足りない。自明なことは初回でも自明だから。
    obvious: list[str] = []
    try:
        r1 = client.models.generate_content(
            model=MODEL,
            contents=_OBVIOUS_PROMPT.format(data=data_json),
            config=types.GenerateContentConfig(
                thinking_config=types.ThinkingConfig(thinking_budget=2048),
            ),
        )
        obvious = _extract_json(r1.text).get("obvious", [])
        print(f"🪞 自明性フィルタ: {len(obvious)}件を除外リストに")
    except Exception as e:  # noqa: BLE001
        # 1段目が落ちても本生成は続ける（フィルタが弱くなるだけ）
        print(f"⚠️ 自明性フィルタの生成に失敗（フィルタなしで続行）: {e}")

    mode = (ctx.get("mode") or {}).get("mode", "optimize")
    prompt = "\n".join([
        _MAIN_PROMPT_HEADER,
        _MODE_STABILIZE if mode == "stabilize" else _MODE_OPTIMIZE,
        _OUTPUT_RULES,
        "## already_obvious（これらと同じ内容は書かない）",
        json.dumps(obvious, ensure_ascii=False, indent=2),
        "## データ",
        data_json,
    ])

    r2 = client.models.generate_content(
        model=MODEL,
        contents=prompt,
        config=types.GenerateContentConfig(
            thinking_config=types.ThinkingConfig(thinking_budget=THINKING_BUDGET),
        ),
    )
    return _extract_json(r2.text), obvious


@task(name="Save daily feedback")
def save_daily(target_date: str, messages: list[dict], ctx: dict) -> None:
    import pandas as pd

    now_jst = datetime.datetime.now(JST).replace(tzinfo=None)
    TRINO.execute_action(
        f"DELETE FROM iceberg.life_gold.ai_feedback "
        f"WHERE feedback_date = DATE '{target_date}' AND slot = '{SLOT}'"
    )
    df = pd.DataFrame([{
        "feedback_date": datetime.date.fromisoformat(target_date),
        "slot": SLOT,
        "generated_at": now_jst,
        "messages": json.dumps(messages, ensure_ascii=False),
        "model": MODEL,
        "context_summary": json.dumps(ctx, ensure_ascii=False, default=str),
    }])
    TRINO.insert_table("ai_feedback", "life_gold", df)
    print(f"✅ Saved {len(messages)} messages for {target_date} [{SLOT}] via {MODEL}")


# ─────────────────────────────────────────────────────────────
# フロー
# ─────────────────────────────────────────────────────────────
@flow(name="AI Feedback Daily", log_prints=True)
def ai_feedback_daily_flow(target_date: str | None = None, force: bool = False):
    """1日1回の日次FBを生成する。

    target_date: 保存先の日付（省略時は今日のJST日付）
    force: 既に生成済みでも上書きする
    """
    now_jst = datetime.datetime.now(JST)
    target_date = target_date or now_jst.strftime("%Y-%m-%d")

    if not force and already_generated(target_date):
        print(f"⏭  {target_date} の日次FBは既に生成済み。スキップ（force=True で上書き）")
        return {"skipped": True}

    # 総括するのは昨日1日の行動。昨夜の睡眠は今日の日付側にあるので分けて引く。
    day_date = (
        datetime.datetime.strptime(target_date, "%Y-%m-%d") - datetime.timedelta(days=1)
    ).strftime("%Y-%m-%d")
    night_date = target_date

    print(f"🤖 日次FB生成: save={target_date} day={day_date} night={night_date} model={MODEL}")

    api_key = Secret.load("google-generative-ai-api-key").get()
    # metric は完全な1日で評価したいので day_date を使う
    ctx = build_daily_context(day_date, night_date, eval_date=day_date)
    mode = (ctx.get("mode") or {}).get("mode")
    print(f"   モード: {mode} {(ctx.get('mode') or {}).get('reasons') or ''}")
    print(f"   進行中 issue: {len(ctx.get('active_issues') or [])}件")

    messages, obvious = generate_daily(ctx, api_key)
    save_daily(target_date, messages, ctx)

    # 言及した issue の mention_count を進める。
    # これが閾値を超えて metric が動かなければ、次回 enforce_abandonment が棄却する。
    #
    # LLM に issue_ids を明示させている。本文の文字列マッチで推定していた頃は
    # ほぼ検出できず（title の先頭12文字が本文にそのまま出ることはない）、
    # mention_count が永久に 0 のままで棄却機構が死んでいた。
    known = {i["issue_id"] for i in (ctx.get("active_issues") or [])}
    mentioned = sorted({
        iid
        for m in messages
        for iid in (m.get("issue_ids") or [])
        if iid in known
    })
    for issue_id in mentioned:
        it.bump_mention(issue_id, day_date)
    if mentioned:
        print(f"   mention_count を進めた issue: {mentioned}")

    return {
        "skipped": False,
        "mode": mode,
        "messages": len(messages),
        "obvious_filtered": len(obvious),
        "issues_mentioned": mentioned,
    }


@flow(name="AI Feedback Wake Gate", log_prints=True)
def ai_feedback_wake_gate_flow(force_hour: int = 11):
    """起床を検知したら日次FBを起動する。05:00〜11:00 に15分ごとに回す想定。

    force_hour に達しても起床を検知できなければ、徹夜/未装着として強制生成する
    （本文側で「睡眠データなし」の扱いは既存ロジックが判断する）。
    """
    now_jst = datetime.datetime.now(JST)
    target_date = now_jst.strftime("%Y-%m-%d")

    if already_generated(target_date):
        print(f"⏭  {target_date} は既に生成済み")
        return {"fired": False, "reason": "already_generated"}

    wake = detect_wake(target_date)
    if wake["woke"]:
        print(f"⏰ 起床検知 {wake['wake_ts']}（{wake['dur_min']}分 / ラベル: {wake.get('labels')}）→ 生成")
        # ★サブフローとして呼ばない★
        # `ai_feedback_daily_flow(...)` と呼ぶと Prefect がサブフロー用の task_run を作り、
        # クライアント/サーバのバージョン差で `/api/task_runs/` が 422 になる
        # （2026-08-28 の朝に実際に発生し、3回連続で gate が Failed した）。
        # `.fn` で素の関数として呼べば、内部の task は gate の flow run 配下で走る。
        ai_feedback_daily_flow.fn(target_date=target_date)
        return {"fired": True, "reason": "wake_detected", **wake}

    if now_jst.hour >= force_hour:
        print(f"⏰ {force_hour}時を過ぎたが起床を検知できず（徹夜/未装着/同期遅延）→ 強制生成")
        ai_feedback_daily_flow.fn(target_date=target_date)
        return {"fired": True, "reason": "forced_by_hour"}

    print(f"💤 まだ起床を検知していない（{now_jst:%H:%M}）。{force_hour}時まで待つ")
    return {"fired": False, "reason": "waiting"}


if __name__ == "__main__":
    ai_feedback_daily_flow(force=True)
