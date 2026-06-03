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

    # ─── 3. 時間別アクティビティ（今日・1時間粒度） ─────────────────
    tod_df = TRINO.execute_query(f"""
        SELECT
            hour(time_slot_jst) AS hour_jst,
            cat_main,
            ROUND(SUM(overlap_sec) / 60.0) AS total_minutes
        FROM iceberg.life_gold.mrt_behavior_slots_15m
        WHERE slot_date_jst = DATE '{target_date}'
          AND cat_main NOT IN ('UNOBSERVED', 'UNKNOWN')
        GROUP BY 1, 2
        ORDER BY 1, 3 DESC
    """)

    # ─── 4. 直近のチャット履歴（ユーザー発言のみ）──────────────────────
    chat_df = TRINO.execute_query("""
        SELECT messages_json, CAST(updated_at AS VARCHAR) AS updated_at
        FROM iceberg.life_gold.chat_history
        ORDER BY updated_at DESC
        LIMIT 1
    """)

    # ─── 5. 直近5日分のAI FB（重複防止用）──────────────────────────────
    recent_fb_df = TRINO.execute_query(f"""
        SELECT CAST(feedback_date AS VARCHAR), slot, messages
        FROM iceberg.life_gold.ai_feedback
        WHERE feedback_date BETWEEN DATE '{target_date}' - INTERVAL '5' DAY
                                AND DATE '{target_date}' - INTERVAL '1' DAY
        ORDER BY feedback_date DESC, slot
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

    # ─── 時間別アクティビティ + 行動シグナル ─────────────────────────
    if not tod_df.empty:
        # 時間→カテゴリ→分数 のマップを構築
        by_hour: dict[int, dict[str, int]] = {}
        for _, row in tod_df.iterrows():
            h = int(row.hour_jst)
            cat = str(row.cat_main)
            mins = int(row.total_minutes)
            if h not in by_hour:
                by_hour[h] = {}
            by_hour[h][cat] = mins

        ctx["today"]["activity_by_hour"] = {
            str(h): cats for h, cats in sorted(by_hour.items())
        }

        # ── 行動シグナルを Python で計算 ──────────────────────────────
        signals: dict = {}

        # 起床時刻の推定: 5〜12時の中でSLEEPが30分未満の最初の時間帯
        wake_hours = [
            h for h in range(5, 13)
            if h in by_hour and by_hour[h].get("SLEEP", 60) < 30
        ]
        if wake_hours:
            signals["estimated_wake_hour"] = min(wake_hours)

        # 就寝時刻の推定: 20〜04時の中でSLEEPが30分以上の最初の時間帯
        for h in list(range(20, 24)) + list(range(0, 5)):
            if by_hour.get(h, {}).get("SLEEP", 0) >= 30:
                signals["estimated_bedtime_hour"] = h
                break

        # 夕方の昼寝検出: 12〜20時のSLEEP合計
        nap_mins = sum(by_hour.get(h, {}).get("SLEEP", 0) for h in range(12, 21))
        if nap_mins >= 20:
            signals["afternoon_nap_minutes"] = nap_mins
            signals["nap_note"] = (
                f"12〜20時にSLEEPが{nap_mins}分検出。"
                "夕方の仮眠はメラトニン分泌を乱し、夜の入眠を遅らせるリスクあり。"
                "Fitbitの総睡眠時間にこの仮眠が含まれている可能性がある。"
            )

        # 深夜スクリーンタイム（22時以降のGAME/MEDIA/BROWSING）
        screen_cats = {"GAME", "MEDIA", "MANGA", "BROWSING"}
        late_screen: dict[str, int] = {}
        for h in range(22, 24):
            for cat in screen_cats:
                m = by_hour.get(h, {}).get(cat, 0)
                if m > 0:
                    late_screen[cat] = late_screen.get(cat, 0) + m
        if late_screen:
            total_late = sum(late_screen.values())
            signals["late_night_screen"] = {
                "total_minutes": total_late,
                "breakdown": late_screen,
                "note": (
                    f"22時以降に画面系が{total_late}分（{late_screen}）。"
                    "ブルーライト・認知覚醒によりメラトニン抑制→入眠遅延・睡眠浅化のリスク。"
                ),
            }

        # 深夜活動（0〜03時）: ゲーム・開発・メディアなど
        very_late: dict[str, int] = {}
        for h in range(0, 4):
            for cat, m in by_hour.get(h, {}).items():
                if cat != "SLEEP" and m > 0:
                    very_late[cat] = very_late.get(cat, 0) + m
        if very_late:
            signals["past_midnight_activity"] = {
                "breakdown": very_late,
                "note": "0〜3時台に活動あり。概日リズムの深夜相への後退は慢性的な睡眠負債を招きやすい。",
            }

        if signals:
            ctx["today"]["sleep_behavior_signals"] = signals

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
        insights: dict = {}
        today_work = ctx.get("today", {}).get("work", {})

        # ── 連続睡眠不足日数（7時間未満）──
        streak = 0
        for d in reversed(trend_list):
            if d["sleep_hours"] and d["sleep_hours"] < 7:
                streak += 1
            elif d["sleep_hours"] and d["sleep_hours"] >= 7:
                break
        if streak > 0:
            insights["consecutive_sleep_deficit_days"] = streak

        # ── 直近3日の睡眠トレンド（増減方向）──
        recent3_sleep = [d["sleep_hours"] for d in trend_list[-3:] if d["sleep_hours"] and d["sleep_hours"] > 1]
        if len(recent3_sleep) == 3:
            if recent3_sleep[0] > recent3_sleep[1] > recent3_sleep[2]:
                insights["sleep_trend_3d"] = "declining"
            elif recent3_sleep[0] < recent3_sleep[1] < recent3_sleep[2]:
                insights["sleep_trend_3d"] = "improving"

        # ── 安静時心拍数の3日トレンド（上昇は疲労蓄積のサイン）──
        recent3_hr = [d["resting_hr"] for d in trend_list[-3:] if d["resting_hr"]]
        if len(recent3_hr) == 3 and all(recent3_hr):
            if recent3_hr[2] - recent3_hr[0] >= 5:
                insights["resting_hr_rising_3d"] = {
                    "from": recent3_hr[0],
                    "to": recent3_hr[2],
                    "note": "安静時心拍数が3日間で5bpm以上上昇 — 疲労蓄積・体調悪化の早期サイン",
                }

        # ── 睡眠 → 翌日ワークスコアの相関 ──
        sleep_seq = [d["sleep_hours"] for d in trend_list[:-1]]
        work_next = [trend_list[i + 1]["work_score"] for i in range(len(trend_list) - 1)]
        corr_sw = _correlation(sleep_seq, work_next)
        if corr_sw is not None:
            insights["sleep_to_next_day_work_corr"] = corr_sw
            if abs(corr_sw) >= 0.4:
                note = "睡眠が長いほど翌日の集中スコアが高い傾向" if corr_sw > 0 else "睡眠が長いほど翌日の集中スコアが低い傾向（夜型の可能性）"
                insights["sleep_work_corr_note"] = note

        # ── 歩数 → 睡眠品質の相関 ──
        steps_seq = [d["steps"] for d in trend_list[:-1]]
        sleep_next = [trend_list[i + 1]["sleep_hours"] for i in range(len(trend_list) - 1)]
        corr_ps = _correlation(steps_seq, sleep_next)
        if corr_ps is not None and abs(corr_ps) >= 0.4:
            note = "歩数が多い日は翌日の睡眠が長くなる傾向" if corr_ps > 0 else "歩数が多い日は翌日の睡眠が短くなる傾向"
            insights["steps_to_next_sleep_corr"] = {"value": corr_ps, "note": note}

        # ── ワークスコアの高日と低日の睡眠比較 ──
        high_work_days = [d for d in trend_list if d["work_score"] is not None and d["sleep_hours"] and d["work_score"] >= 60]
        low_work_days = [d for d in trend_list if d["work_score"] is not None and d["sleep_hours"] and d["work_score"] < 30]
        if len(high_work_days) >= 2 and len(low_work_days) >= 2:
            avg_sleep_high = sum(d["sleep_hours"] for d in high_work_days) / len(high_work_days)
            avg_sleep_low = sum(d["sleep_hours"] for d in low_work_days) / len(low_work_days)
            insights["sleep_diff_by_work_performance"] = {
                "avg_sleep_when_high_work": round(avg_sleep_high, 1),
                "avg_sleep_when_low_work": round(avg_sleep_low, 1),
                "note": f"集中スコア60超の日は平均{avg_sleep_high:.1f}h睡眠、30未満の日は{avg_sleep_low:.1f}h",
            }

        # ── 7日平均ワークスコアと今日の比較 ──
        recent7_work = [d["work_score"] for d in trend_list[-7:] if d["work_score"] is not None]
        if recent7_work and today_work.get("work_score") is not None:
            avg7_work = sum(recent7_work) / len(recent7_work)
            insights["work_score_vs_7d_avg"] = {
                "avg_7d": round(avg7_work, 1),
                "today": today_work["work_score"],
                "change": _pct_change(today_work["work_score"], avg7_work),
            }

        # ── 前半7日 vs 後半7日のワークスコア比較 ──
        prev7_work = [d["work_score"] for d in trend_list[:7] if d["work_score"] is not None]
        curr7_work = [d["work_score"] for d in trend_list[7:] if d["work_score"] is not None]
        if prev7_work and curr7_work:
            avg_prev = sum(prev7_work) / len(prev7_work)
            avg_curr = sum(curr7_work) / len(curr7_work)
            insights["work_score_week_over_week"] = {
                "prev7d_avg": round(avg_prev, 1),
                "curr7d_avg": round(avg_curr, 1),
                "change": _pct_change(avg_curr, avg_prev),
            }

        if insights:
            ctx["computed_insights"] = insights

    # ─── 直近5日分のAI FBを整形（重複防止用）────────────────────────
    if not recent_fb_df.empty:
        past_fb_entries = []
        for _, row in recent_fb_df.iterrows():
            try:
                msgs = json.loads(str(row.messages))
                texts = [m.get("message", "") for m in msgs if isinstance(m, dict)]
                past_fb_entries.append({
                    "date": str(row[0])[:10],
                    "slot": str(row.slot),
                    "messages": texts,
                })
            except Exception:
                continue
        if past_fb_entries:
            ctx["recent_feedback_history"] = past_fb_entries

    # ─── 直近チャット：ユーザー発言のみ抽出 ──────────────────────────
    if not chat_df.empty:
        try:
            row = chat_df.iloc[0]
            all_messages = json.loads(str(row.messages_json))
            # ユーザー発言のみ・直近15件のテキストを抽出
            user_texts = []
            for m in all_messages:
                if m.get("role") != "user":
                    continue
                parts = m.get("parts", [])
                text = " ".join(p.get("text", "") for p in parts if p.get("type") == "text").strip()
                if text:
                    user_texts.append(text)
            if user_texts:
                ctx["recent_user_statements"] = {
                    "note": (
                        "これはユーザーとのチャット履歴から抽出したユーザー発言（直近15件）。"
                        "個別メッセージの日時は不明だが、おおむね直近数日〜数週間の発言。"
                        "ユーザーの主観的な状態・悩み・出来事を把握するための補助情報として使うこと。"
                        "データと矛盾する場合はデータを優先。過去の発言内容を今日の状態に安易に投影しないこと。"
                    ),
                    "history_updated_at": str(row.updated_at)[:10],
                    "statements": user_texts[-15:],
                }
        except Exception as e:
            print(f"[chat_history] parse error: {e}")

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
                "sleep_hoursがnullまたは0の場合の判断："
                "  - activity_by_time.eveningにDEVELOP/MEDIA/GAME/MANGA/BROWSINGなど"
                "    夜間活動が合計1時間以上ある → 徹夜・遅くまで起きていた可能性が高いので"
                "    その観点でコメントしてよい（例：夜遅くまで開発していた→今日は休息を）。"
                "  - 上記の夜間活動がない → 睡眠データが未同期なだけなので睡眠コメント禁止。"
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
あなたは医学・健康科学・行動心理学の知識を持つパーソナルライフアナリストです。
ユーザーの日々のライフログを深く読み解き、表面的な数値ではなく「いつ・何が・なぜ起きているか」を把握した上で、本当に価値のあるインサイトだけを届けます。

## 分析タイミング
{slot_context["label"]} — {slot_context["focus"]}

## データ解釈の前提
{slot_context["data_note"]}

## トーン指針
{slot_context["tone"]}

## データ（JSON）
{json.dumps(ctx, ensure_ascii=False, indent=2)}

---

## 新鮮さの原則（最重要）

`recent_feedback_history` に直近5日間の過去FBが含まれている。**これらで伝えた内容は繰り返さない。**

- **相関・傾向の洞察（「あなたのデータでは○○の傾向がある」）**: 14日間データから算出されるため毎日同じ結論になりやすい。今日のデータがその傾向の具体的な新例（例: 昨日まさに睡眠が短くて今日のスコアが下がった）であれば言及してよいが、「傾向がある」という抽象的な説明だけなら繰り返さない。
- **危険サイン（danger）**: 状況が継続している間は毎日言及してよい。ただし言い回しや角度を変える。
- **今日固有の変化・出来事を優先**: 昨日と何が違うか、今日だけに見える特徴は何かを中心に組み立てる。

「昨日も同じことを言ったが今日も同じ」ならその洞察は今日は不要。

## タイムラインデータの読み方（必ず活用すること）

`activity_by_hour` は 0〜23時の時間帯別カテゴリ分布（分単位）。`sleep_behavior_signals` はそこから計算済みの行動シグナル。

**これらを使って以下を判断する（数値だけでなくタイミングを見ること）：**

- **睡眠の質の文脈化**: 総睡眠時間が長くても、`afternoon_nap_minutes` があれば夕方の仮眠が含まれている可能性。夜間の本睡眠時間は実質的に短い場合がある。
- **就寝・起床のリズム**: `estimated_bedtime_hour` と `estimated_wake_hour` から概日リズムの乱れを判断。23時就寝と02時就寝では同じ7時間睡眠でも睡眠の質と翌日パフォーマンスに大きな差がある（概日リズムの位相）。
- **就寝前スクリーン暴露**: `late_night_screen` が存在する場合、ブルーライトと認知覚醒によるメラトニン抑制を考慮。ゲームは特に交感神経を活性化する。
- **深夜活動**: `past_midnight_activity` がある場合、概日リズムへの影響と翌日の慢性的な眠気・集中力低下を言及する根拠になる。

## 科学的分析フレームワーク（必要に応じて活用する）

- **睡眠科学**: 深睡眠は前半の睡眠サイクルに集中。就寝が遅いほど深睡眠が削られる。7〜9時間が推奨だが「いつ寝るか」も重要。
- **安静時心拍数**: 3日連続上昇 → 副交感神経活性の低下・疲労蓄積の客観指標。5bpm以上の急上昇はオーバートレーニングや体調悪化の早期サイン。
- **集中力と睡眠の関係**: 前日の睡眠不足（特に深睡眠の不足）は作業記憶・実行機能を低下させ、翌日の集中スコアに直結。
- **運動と睡眠**: 適度な有酸素運動（歩数増加など）は深睡眠を増加させる。ただし就寝直前（2〜3時間以内）の激しい運動は逆効果。
- **食事と認知**: タンパク質はドーパミン・セロトニンの前駆体。食物繊維不足は腸内環境→脳腸軸を介して気分・集中力に影響。

## チャット履歴の使い方（`recent_user_statements` が存在する場合）

ユーザーの主観的な発言から、データに表れていない定性的な状態を補助的に参照する。

**使って良いこと：**
- 「ユーザーが体調不良を訴えていた」→ 安静時心拍上昇や集中力低下との因果推論に活かす
- 「気分転換した・楽しいことがあった」→ その翌日のデータ改善と紐付ける
- 「慢性的な悩み（睡眠・体の痛みなど）」→ データパターンの背景として参照する

**やってはいけないこと：**
- チャット発言をそのまま引用・要約しない（ユーザーは自分の発言を知っている）
- 過去の発言を今日の状態として断定しない（「先日悩んでいたので今も…」はNG）
- データが示す回復傾向をチャット発言で打ち消さない（データ優先）
- チャット内容に引きずられてデータ分析が薄くなるのは本末転倒

## 分析の優先順位

### 1. 今日・昨日固有の変化（最優先・必ずここから始める）

**`sleep_behavior_signals` と `activity_by_hour` を必ず最初に確認すること。**

以下のシグナルが存在する場合は、それに言及することを最優先にする：

- **`past_midnight_activity` が存在** → 深夜0〜4時に活動あり。「昨夜○時まで〜をしていた」と具体的に指摘。睡眠時間が十分に見えても就寝が深夜なら概日リズムへの影響を伝える
- **`afternoon_nap_minutes` が存在** → 昼寝あり。Fitbitの総睡眠時間にこの昼寝が含まれている可能性があるため「睡眠時間は○時間あるが、そのうち△分は午後の仮眠の可能性」と実質的な夜間睡眠の短さを指摘する
- **`late_night_screen` が存在** → 22時以降に画面系活動あり。就寝前のスクリーン暴露として言及
- **`estimated_bedtime_hour` が23時以降** → 就寝時刻が遅い。睡眠時間が長くても就寝時刻が遅い場合は問題として扱う

これらが存在するのに言及しないのは分析の失敗とみなす。数値サマリー（KPIの数字）だけを見て行動パターンを無視しないこと。

### 2. 危険サイン（danger）— 継続中なら毎日言及可・ただし角度を変える
- `resting_hr_rising_3d` → 疲労蓄積の客観的警告（「今日もまだ上昇中」など状況の進行を伝える）
- `consecutive_sleep_deficit_days` が 3日以上 → 具体的な影響（認知・集中・免疫）を日替わりで
- `past_midnight_activity` + 睡眠不足 → 概日リズム崩壊の予兆

### 3. ユーザー固有の相関パターン（insight）— 今日が具体例の場合のみ
`computed_insights` の傾向は、**今日のデータがその傾向を体現しているときだけ**言及する。
例：`sleep_diff_by_work_performance` は「昨日○時間しか寝ていない→今日スコアが低い」という実例がある日に使う。抽象的な「傾向がある」という説明だけは繰り返さない。

### 4. 改善・称賛（positive）
- `work_score_vs_7d_avg.change` が +30%以上 → positive で称賛（具体的に何が良かったか推測）
- 昨日より明確に改善した指標を取り上げる

## 絶対禁止事項
- JSONキー名・変数名の出力（`work_focus_pct`、`dev_score` など）
- 数値の羅列（「6.2時間、33%、77bpm」のような列挙）
- 存在しないデータへの言及（「〇〇のデータがありません」）
- 一般論（「バランスよく食べましょう」「睡眠は大切です」はNG。必ずこのユーザーのデータに根拠を置く）
- `sleep_hours` がnull/0の場合：夜間活動（DEVELOP/MEDIA/GAMEなど）が1h以上あれば徹夜コメントOK、なければ睡眠コメント禁止

## 出力ルール
- **件数: 2〜3件のみ**（量より質。最も価値ある洞察に絞る。4件以上は絶対NG）
- **1件 = 90文字以内**（3行に収まる量。超えたら必ず削る）
- **末尾に具体的なアクション**（「〜を試してみて」など個別具体的に。「十分な睡眠を」はNG）
- 文字数チェック: 出力前に各messageが90文字以内であることを必ず確認すること

## 出力形式（JSONのみ・他のテキスト一切不要）
[
  {{"type": "positive"|"warning"|"danger"|"insight", "message": "1〜2文・アクション付き・150文字以内"}}
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
