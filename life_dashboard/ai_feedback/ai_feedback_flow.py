import json
import math
import os
import sys
import datetime
from zoneinfo import ZoneInfo

from google import genai
from google.genai import types
from prefect import flow, task
from prefect.blocks.system import Secret

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.trino_api import TrinoAPI

JST = ZoneInfo("Asia/Tokyo")
TRINO = TrinoAPI(host="trino.mynet", port=80, user="tig", catalog="iceberg")
MODEL = "gemini-2.5-flash"


def _get_slot(hour: int) -> str:
    if hour < 12:
        return "morning"
    elif hour < 18:
        return "noon"
    return "night"


def _safe_float(v) -> float | None:
    try:
        f = float(v)
        return None if f == 0 and v == 0 else f
    except (TypeError, ValueError):
        return None


def _safe_int(v) -> int | None:
    if v is None:
        return None
    try:
        f = float(v)
        return None if math.isnan(f) else int(f)
    except (TypeError, ValueError):
        return None


def _pct_change(current, baseline) -> str | None:
    """現在値とベースラインの%変化を文字列で返す（例: '+12%', '-8%'）"""
    if current is None or baseline is None or baseline == 0:
        return None
    pct = (current - baseline) / abs(baseline) * 100
    return f"{pct:+.0f}%"


def _correlation(xs: list, ys: list) -> float | None:
    """ピアソン相関係数（ペア数が4未満なら None）"""
    pairs = [(x, y) for x, y in zip(xs, ys) if x is not None and y is not None]
    n = len(pairs)
    if n < 4:
        return None
    mean_x = sum(p[0] for p in pairs) / n
    mean_y = sum(p[1] for p in pairs) / n
    num = sum((p[0] - mean_x) * (p[1] - mean_y) for p in pairs)
    den_x = sum((p[0] - mean_x) ** 2 for p in pairs) ** 0.5
    den_y = sum((p[1] - mean_y) ** 2 for p in pairs) ** 0.5
    if den_x == 0 or den_y == 0:
        return None
    return round(num / (den_x * den_y), 2)


@task(name="Fetch enriched daily context from Trino")
def fetch_context(target_date: str) -> dict:
    # ─── 1. 今日の詳細データ ───────────────────────────────────────
    fitness_df = TRINO.execute_query(f"""
        SELECT steps, calories_out, calories_in, net_calorie_balance,
               total_minutes_asleep, total_time_in_bed,
               sleep_deep_minutes, sleep_rem_minutes, sleep_light_minutes,
               resting_heart_rate, weight_kg, weight_7d_avg,
               activity_logs_str
        FROM iceberg.life_gold.mrt_fitness_daily_summary
        WHERE target_date = DATE '{target_date}'
    """)

    work_df = TRINO.execute_query(f"""
        SELECT work_core_sec, work_score, work_focus_rate, work_apps_str,
               dev_core_sec, dev_score, dev_focus_rate, dev_apps_str
        FROM iceberg.life_gold.mrt_aw_daily_work_summary
        WHERE target_date = DATE '{target_date}'
    """)

    meals_df = TRINO.execute_query(f"""
        SELECT calories_kcal, protein_g, fat_g, carbs_g, fiber_g, salt_g,
               breakfast_items, breakfast_calories,
               lunch_items, lunch_calories,
               dinner_items, dinner_calories,
               snack_items, snack_calories
        FROM iceberg.life_gold.mrt_asken
        WHERE target_date = DATE '{target_date}'
    """)

    # ─── 2. 14日間トレンド（fitness + work を結合） ─────────────────
    trend_df = TRINO.execute_query(f"""
        SELECT f.target_date,
               f.steps,
               f.total_minutes_asleep,
               f.net_calorie_balance,
               f.resting_heart_rate,
               w.work_score,
               w.dev_score
        FROM iceberg.life_gold.mrt_fitness_daily_summary f
        LEFT JOIN iceberg.life_gold.mrt_aw_daily_work_summary w
          ON f.target_date = w.target_date
        WHERE f.target_date BETWEEN DATE '{target_date}' - INTERVAL '13' DAY
                                AND DATE '{target_date}'
        ORDER BY f.target_date
    """)

    # ─── 3. 時間帯別アクティビティ（今日） ─────────────────────────
    tod_df = TRINO.execute_query(f"""
        SELECT
            CASE
                WHEN hour(time_slot_jst) < 12 THEN 'morning'
                WHEN hour(time_slot_jst) < 18 THEN 'afternoon'
                ELSE 'evening'
            END AS time_of_day,
            cat_main,
            ROUND(SUM(overlap_sec) / 3600.0, 2) AS hours
        FROM iceberg.life_gold.mrt_behavior_slots_15m
        WHERE slot_date_jst = DATE '{target_date}'
          AND cat_main NOT IN ('UNOBSERVED', 'UNKNOWN')
        GROUP BY 1, 2
        ORDER BY 1, 3 DESC
    """)

    ctx: dict = {"date": target_date}

    # ─── 今日のデータを組み立て ────────────────────────────────────
    if not fitness_df.empty:
        r = fitness_df.iloc[0]
        sleep_mins = r.total_minutes_asleep or 0
        in_bed = r.total_time_in_bed or 0
        ctx["today"] = {
            "steps": int(r.steps) if r.steps else None,
            "sleep_hours": round(sleep_mins / 60, 1) if sleep_mins else None,
            "sleep_efficiency_pct": round(sleep_mins / in_bed * 100) if in_bed else None,
            "sleep_deep_min": int(r.sleep_deep_minutes) if r.sleep_deep_minutes else None,
            "sleep_rem_min": int(r.sleep_rem_minutes) if r.sleep_rem_minutes else None,
            "sleep_light_min": int(r.sleep_light_minutes) if r.sleep_light_minutes else None,
            "net_calories": int(r.net_calorie_balance) if r.net_calorie_balance is not None else None,
            "calories_out": int(r.calories_out) if r.calories_out else None,
            "calories_in": int(r.calories_in) if r.calories_in else None,
            "resting_hr": int(r.resting_heart_rate) if r.resting_heart_rate else None,
            "weight_kg": float(r.weight_kg) if r.weight_kg is not None else None,
            "weight_7d_avg": float(r.weight_7d_avg) if r.weight_7d_avg is not None else None,
            "activity_logs": r.activity_logs_str,
        }

    if not work_df.empty:
        r = work_df.iloc[0]
        ctx["today"]["work"] = {
            "work_score": int(r.work_score),
            "work_focus_pct": int(r.work_focus_rate),
            "work_hours": round(int(r.work_core_sec) / 3600, 1),
            "work_apps": r.work_apps_str,
            "dev_score": int(r.dev_score),
            "dev_focus_pct": int(r.dev_focus_rate),
            "dev_hours": round(int(r.dev_core_sec) / 3600, 1),
            "dev_apps": r.dev_apps_str,
        }

    if not meals_df.empty:
        r = meals_df.iloc[0]
        ctx["today"]["meals"] = {
            "total_kcal": float(r.calories_kcal) if r.calories_kcal is not None else None,
            "protein_g": float(r.protein_g) if r.protein_g is not None else None,
            "fat_g": float(r.fat_g) if r.fat_g is not None else None,
            "carbs_g": float(r.carbs_g) if r.carbs_g is not None else None,
            "fiber_g": float(r.fiber_g) if r.fiber_g is not None else None,
            "salt_g": float(r.salt_g) if r.salt_g is not None else None,
            "breakfast": f"{r.breakfast_items} ({int(r.breakfast_calories)}kcal)" if r.breakfast_items else None,
            "lunch": f"{r.lunch_items} ({int(r.lunch_calories)}kcal)" if r.lunch_items else None,
            "dinner": f"{r.dinner_items} ({int(r.dinner_calories)}kcal)" if r.dinner_items else None,
            "snack": f"{r.snack_items} ({int(r.snack_calories)}kcal)" if r.snack_items else None,
        }

    # ─── 時間帯別アクティビティ ────────────────────────────────────
    if not tod_df.empty:
        tod: dict = {"morning": {}, "afternoon": {}, "evening": {}}
        for _, row in tod_df.iterrows():
            tod[row.time_of_day][row.cat_main] = float(row.hours)
        ctx["today"]["activity_by_time"] = tod

    # ─── 14日間トレンド ───────────────────────────────────────────
    if not trend_df.empty:
        trend_list = []
        for _, row in trend_df.iterrows():
            sleep_h = round(row.total_minutes_asleep / 60, 1) if row.total_minutes_asleep else None
            trend_list.append({
                "date": str(row.target_date),
                "steps": _safe_int(row.steps),
                "sleep_hours": sleep_h,
                "net_calories": _safe_int(row.net_calorie_balance),
                "resting_hr": _safe_int(row.resting_heart_rate),
                "work_score": _safe_int(row.work_score),
                "dev_score": _safe_int(row.dev_score),
            })
        ctx["trends_14d"] = trend_list

        # ─── Python 計算インサイト ──────────────────────────────────
        valid = [d for d in trend_list if d["sleep_hours"] and d["sleep_hours"] > 1]
        sleep_vals = [d["sleep_hours"] for d in valid]
        work_vals = [d["work_score"] for d in valid]

        insights: dict = {}

        # 7日平均との比較
        recent7 = [d for d in trend_list[-7:] if d["sleep_hours"] and d["sleep_hours"] > 1]
        if recent7 and ctx.get("today", {}).get("sleep_hours"):
            avg7_sleep = sum(d["sleep_hours"] for d in recent7) / len(recent7)
            insights["sleep_vs_7d_avg"] = {
                "avg_7d": round(avg7_sleep, 1),
                "change": _pct_change(ctx["today"]["sleep_hours"], avg7_sleep),
            }

        recent7_steps = [d["steps"] for d in trend_list[-7:] if d["steps"]]
        if recent7_steps and ctx.get("today", {}).get("steps"):
            avg7_steps = sum(recent7_steps) / len(recent7_steps)
            insights["steps_vs_7d_avg"] = {
                "avg_7d": int(avg7_steps),
                "change": _pct_change(ctx["today"]["steps"], avg7_steps),
            }

        # 連続睡眠不足日数（7時間未満）
        streak = 0
        for d in reversed(trend_list):
            if d["sleep_hours"] and d["sleep_hours"] < 7:
                streak += 1
            elif d["sleep_hours"] and d["sleep_hours"] >= 7:
                break
        if streak > 0:
            insights["consecutive_sleep_deficit_days"] = streak

        # 睡眠 → 翌日ワークスコアの相関（当日sleep vs 翌日work）
        sleep_seq = [d["sleep_hours"] for d in trend_list[:-1]]
        work_next = [trend_list[i + 1]["work_score"] for i in range(len(trend_list) - 1)]
        corr = _correlation(sleep_seq, work_next)
        if corr is not None:
            insights["sleep_to_next_day_work_correlation"] = corr
            if abs(corr) >= 0.4:
                direction = "正の相関（睡眠が長いほど翌日の集中スコアが高い）" if corr > 0 else "負の相関"
                insights["sleep_work_correlation_note"] = direction

        if insights:
            ctx["computed_insights"] = insights

    return ctx


@task(name="Generate AI feedback via Gemini", retries=2, retry_delay_seconds=30)
def generate_feedback(ctx: dict, slot: str, api_key: str) -> list[dict]:
    client = genai.Client(api_key=api_key)

    slot_context = {
        "morning": {
            "label": "朝（8時）",
            "focus": "昨日一日の総括と今日へのアドバイス",
            "data_note": (
                "朝8時時点での分析。データは昨日分（前日の終日データ）を使用している。"
                "sleep_hoursがnullまたは0の場合、睡眠データがまだ同期されていないため"
                "睡眠に関するコメントを一切生成してはならない。"
                "今日のデータがないことは言及しないこと。"
            ),
            "tone": "昨日を踏まえた今日への具体的な注意点・良かった点・励ましを伝える。前向きなトーンを大切に。",
        },
        "noon": {
            "label": "昼（13時）",
            "focus": "今日の午前の状況確認と午後へのアドバイス",
            "data_note": (
                "昼13時時点なので今日の午前中のデータが中心。"
                "午後のデータがないことは言及しないこと。"
            ),
            "tone": "午前の状況を踏まえた午後の過ごし方を提案する。必要なら休憩・食事・運動なども。",
        },
        "night": {
            "label": "夜（22時）",
            "focus": "今日一日の総括と明日への改善提案",
            "data_note": "今日のデータが揃っている前提で分析する。",
            "tone": "今日を振り返り、明日につながる具体的な改善点を伝える。良かった点も必ず1つ添える。",
        },
    }[slot]

    prompt = f"""
あなたはライフログ分析AIです。以下のデータを見て、**本当に生活改善につながる**ことだけをフィードバックしてください。

## 分析タイミング
{slot_context["label"]} — {slot_context["focus"]}

## データ解釈の前提
{slot_context["data_note"]}

## トーン指針
{slot_context["tone"]}

## データ（JSON）
{json.dumps(ctx, ensure_ascii=False, indent=2)}

## 出力ルール（厳守）
- **件数**: 最大3件。重要なものがなければ1〜2件で良い。
- **1件 = 1文**（60文字以内）
- **末尾に必ずアクション**を入れる（「〜してください」「〜を試してください」）
- **数値比較は書かない**（「昨日比-8%」「平均より低い」などは禁止）
- **存在しないデータへの言及禁止**（「〇〇のデータがありません」はNG）
- **sleep_hoursがnull/0なら睡眠コメント完全禁止**（データが未同期のため）
- **省略基準**: 軽微な変化・一般論・誰でも言えること → 書かない

## 取り上げる価値がある例
- 睡眠が明らかに足りていない（6h未満）→ 具体的な回復策
- 運動が数日ゼロ → 今すぐできる軽い運動を提案
- 特定の時間帯に集中が極端に落ちている → その時間帯の対策
- 食事のカロリーや栄養が著しく偏っている → 具体的な食品提案

## 出力形式（JSONのみ・他のテキスト一切不要）
[
  {{"type": "positive"|"warning"|"insight", "message": "1文・アクション付き・60字以内"}}
]
"""

    response = client.models.generate_content(
        model=MODEL,
        contents=prompt,
        config=types.GenerateContentConfig(
            thinking_config=types.ThinkingConfig(thinking_budget=1024),
        ),
    )
    text = response.text.strip()

    if "```" in text:
        parts = text.split("```")
        text = parts[1] if len(parts) > 1 else parts[0]
        if text.startswith("json"):
            text = text[4:]
    text = text.strip()

    return json.loads(text)


@task(name="Save feedback to Iceberg")
def save_feedback(target_date: str, slot: str, messages: list[dict], ctx: dict):
    import pandas as pd

    now_jst = datetime.datetime.now(JST).replace(tzinfo=None)

    TRINO.execute_action(
        f"DELETE FROM iceberg.life_gold.ai_feedback "
        f"WHERE feedback_date = DATE '{target_date}' AND slot = '{slot}'"
    )

    df = pd.DataFrame([{
        "feedback_date": datetime.date.fromisoformat(target_date),
        "slot": slot,
        "generated_at": now_jst,
        "messages": json.dumps(messages, ensure_ascii=False),
        "model": MODEL,
        "context_summary": json.dumps(ctx, ensure_ascii=False),
    }])

    TRINO.insert_table("ai_feedback", "life_gold", df)
    print(f"✅ Saved {len(messages)} messages for {target_date} [{slot}] via {MODEL}")


@flow(name="AI Feedback Generator")
def ai_feedback_flow(target_date: str | None = None, slot: str | None = None):
    """
    target_date: YYYY-MM-DD（省略時は今日のJST日付）
    slot: 'morning' | 'noon' | 'night'（省略時は現在時刻から自動判定）
    """
    now_jst = datetime.datetime.now(JST)

    if target_date is None:
        target_date = now_jst.strftime("%Y-%m-%d")
    if slot is None:
        slot = _get_slot(now_jst.hour)

    # 朝スロット: FitbitはNight sleepを開始日に記録するため昨日の日付でデータ取得
    # （保存はtoday = 今日の日付のまま、表示も今日のFBとして扱う）
    if slot == "morning":
        analysis_date = (
            datetime.datetime.strptime(target_date, "%Y-%m-%d") - datetime.timedelta(days=1)
        ).strftime("%Y-%m-%d")
    else:
        analysis_date = target_date

    print(f"🤖 Generating AI feedback: {target_date} [{slot}] analysis_date={analysis_date} model={MODEL}")

    api_key = Secret.load("google-generative-ai-api-key").get()
    ctx = fetch_context(analysis_date)
    messages = generate_feedback(ctx, slot, api_key)
    save_feedback(target_date, slot, messages, ctx)


if __name__ == "__main__":
    ai_feedback_flow()
