"""週次 AI フィードバック（月曜朝・処方の唯一の発生源）。

日次と週次の役割分担:
  日次 … 進行中の実験の実況。処方は出さない（出しても最大1つ）
  週次 … 7日を俯瞰して洞察を出し、**今週の実験を1つだけ決める**

なぜ分けるか:
  生活習慣は日次では変化しない。日次に洞察を求めると必ず同じ話の言い換えになる
  （旧実装は直近30日70件のうち81%が「深夜」に言及していた）。
  処方の発生源を週1に絞れば、日替わりで浅い処方が量産されなくなる。

週次だけが持つ能力:
  1. SQL 探索権限   … 固定コンテキストへのコメントではなく、自分で掘る
  2. Web 検索       … 自分の数値に外部知識を接続する（順序は「数値が先」）
  3. 介入日での前後比較 … ai_interventions を基準日にして効果を測る
  4. 完全性クリティック … 「今週の分析は何を見落としたか」を最後に問う
"""

import datetime
import json
from zoneinfo import ZoneInfo

from google import genai
from google.genai import types
from prefect import flow, task
from prefect.blocks.system import Secret

from ai_feedback import discovery
from ai_feedback import issue_tracker as it
from ai_feedback import sql_tool
from common.trino_api import TrinoAPI

JST = ZoneInfo("Asia/Tokyo")
TRINO = TrinoAPI(host="trino.mynet", port=80, user="tig", catalog="iceberg")

MODEL = "gemini-pro-latest"
THINKING_BUDGET = 16384      # 週1回なので厚く使う
MAX_TOOL_TURNS = 20          # SQL 探索の往復上限
SLOT = "weekly"


# ─────────────────────────────────────────────────────────────
# コンテキスト
# ─────────────────────────────────────────────────────────────
@task(name="Fetch weekly comparison")
def fetch_weekly_comparison(week_end: str) -> dict:
    """直近7日 vs その前7日。日次では見えない変化を拾うため。

    ★指標ごとに n を出す理由★
    当初は `days: 7` だけを返して各指標の平均を並べていた。しかし欠測がある指標は
    **別々の部分集合で平均される**ため、比較が成立しない。
    実害が出た: 深睡眠が v1 由来で prev 97.3分(欠測3日) → this 55.6分(欠測2日) となり、
    LLM が「半減した」と結論して、それを根拠に週の実験を立ててしまった。
    v1.2 の段階データで見ると prev 61.3分 / this 61.2分 で**変化していなかった**。

    対策:
      ・指標ごとに有効日数(n_*)を併記して、平均の土台が違うことを見えるようにする
      ・睡眠の質は v1.2 段階データ(fitbit_sleep_stages)を主に使う
        （v1 の sleep_deep_minutes は欠測が多く restless も含む）
    """
    df = TRINO.execute_query(f"""
        WITH d AS (
            SELECT f.target_date,
                   CASE WHEN f.target_date > DATE '{week_end}' - INTERVAL '7' DAY
                        THEN 'this_week' ELSE 'prev_week' END AS wk,
                   f.total_minutes_asleep, f.sleep_deep_minutes, f.resting_heart_rate,
                   f.steps, f.weight_kg, f.very_active_minutes, f.sedentary_minutes,
                   f.lightly_active_minutes, f.fairly_active_minutes,
                   w.work_core_sec, w.dev_core_sec, w.work_score, w.dev_score,
                   a.calories_kcal, a.protein_g, a.fiber_g, a.salt_g, a.fat_g, a.carbs_g,
                   a.lunch_calories,
                   p.unlock_count, p.unlock_count_00_04, p.longest_no_unlock_gap_min
            FROM iceberg.life_gold.mrt_fitness_daily_summary f
            LEFT JOIN iceberg.life_gold.mrt_aw_daily_work_summary w ON f.target_date = w.target_date
            LEFT JOIN iceberg.life_gold.mrt_asken a ON f.target_date = a.target_date
            LEFT JOIN iceberg.life_gold.mrt_ai_phone_daily p ON f.target_date = p.target_date
            WHERE f.target_date > DATE '{week_end}' - INTERVAL '14' DAY
              AND f.target_date <= DATE '{week_end}'
        )
        SELECT wk, count(*) AS days,
               round(avg(CASE WHEN total_minutes_asleep > 60 THEN total_minutes_asleep END)/60.0, 2) AS sleep_h_v1,
               count_if(total_minutes_asleep > 60) AS n_sleep,
               round(avg(resting_heart_rate) FILTER (WHERE resting_heart_rate > 0), 1) AS rhr,
               count_if(resting_heart_rate > 0) AS n_rhr,
               round(avg(steps), 0) AS steps,
               round(avg(weight_kg), 2) AS weight_kg,
               count_if(weight_kg IS NOT NULL) AS n_weight,
               -- 活動強度。以前は very_active しか渡していなかったが、
               -- 実測で座位が 1076分/日（17.9時間）あり、そちらの方が情報量が大きい。
               round(avg(sedentary_minutes), 0) AS sedentary_min,
               round(avg(lightly_active_minutes), 0) AS light_active_min,
               round(avg(fairly_active_minutes + very_active_minutes), 1) AS mod_vig_active_min,
               round(avg(very_active_minutes), 1) AS very_active_min,
               round(avg(work_core_sec)/3600.0, 2) AS work_h,
               round(avg(dev_core_sec)/3600.0, 2) AS dev_h,
               count_if(work_core_sec IS NOT NULL) AS n_work,
               round(avg(work_score), 1) AS work_score,
               round(avg(calories_kcal), 0) AS cal_in,
               count_if(calories_kcal > 0) AS n_meal_log,
               round(avg(protein_g), 1) AS protein_g,
               round(avg(fiber_g), 1) AS fiber_g,
               -- 塩分は実測 11〜12g/日（目標6g）で大きく外れているのに未使用だった
               round(avg(salt_g), 1) AS salt_g,
               round(avg(fat_g), 1) AS fat_g,
               round(avg(carbs_g), 1) AS carbs_g,
               count_if(lunch_calories > 0) AS n_lunch,
               round(avg(unlock_count), 1) AS unlocks,
               round(avg(unlock_count_00_04), 1) AS unlocks_00_04,
               count_if(unlock_count IS NOT NULL) AS n_phone,
               round(avg(longest_no_unlock_gap_min), 0) AS longest_gap_min
        FROM d GROUP BY wk
    """)
    out = {r["wk"]: {k: v for k, v in r.items() if k != "wk"} for r in it._records(df)}

    # 睡眠の質は v1.2 の段階データで取る。v1 の sleep_deep_minutes は欠測が多く、
    # 週ごとに別の部分集合を平均してしまうため比較に使えない。
    stages = TRINO.execute_query(f"""
        SELECT CASE WHEN stage_date_jst > DATE '{week_end}' - INTERVAL '7' DAY
                    THEN 'this_week' ELSE 'prev_week' END AS wk,
               count(DISTINCT stage_date_jst) AS n_days_with_stages,
               -- 分母を2通り出す。データがある日で割った値（_per_data_day）だけを見ると、
               -- 週によって観測日数が違う場合に見せかけの変化が出る（prev 5日 / this 7日 など）。
               -- 暦日7日で割った値（_per_calendar_day）は欠測を0として扱う保守的な見方。
               -- **両方で同じ方向に動いている変化だけが主張できる。**
               round(sum(CASE WHEN stage = 'deep' THEN stage_seconds END)/60.0
                     / NULLIF(count(DISTINCT stage_date_jst), 0), 1) AS deep_min_per_data_day,
               round(sum(CASE WHEN stage = 'deep' THEN stage_seconds END)/60.0 / 7.0, 1) AS deep_min_per_calendar_day,
               round(sum(CASE WHEN stage <> 'wake' AND NOT is_short_wake THEN stage_seconds END)/60.0
                     / NULLIF(count(DISTINCT stage_date_jst), 0), 1) AS asleep_min_per_data_day,
               round(sum(CASE WHEN stage <> 'wake' AND NOT is_short_wake THEN stage_seconds END)/60.0 / 7.0, 1) AS asleep_min_per_calendar_day,
               round(sum(CASE WHEN is_main_sleep AND stage <> 'wake' AND NOT is_short_wake THEN stage_seconds END)/60.0
                     / NULLIF(count(DISTINCT stage_date_jst), 0), 1) AS main_asleep_per_data_day,
               round(sum(CASE WHEN is_main_sleep AND stage <> 'wake' AND NOT is_short_wake THEN stage_seconds END)/60.0 / 7.0, 1) AS main_asleep_per_calendar_day,
               round(sum(CASE WHEN stage = 'wake' OR is_short_wake THEN stage_seconds END)/60.0
                     / NULLIF(count(DISTINCT stage_date_jst), 0), 1) AS wake_min_per_data_day
        FROM iceberg.life_silver.fitbit_sleep_stages
        WHERE stage_date_jst > DATE '{week_end}' - INTERVAL '14' DAY
          AND stage_date_jst <= DATE '{week_end}'
        GROUP BY 1
    """)
    for r in it._records(stages):
        out.setdefault(r["wk"], {}).update(
            {k: v for k, v in r.items() if k != "wk"}
        )

    # 回復指標（HRV / 呼吸数 / SpO2 / 皮膚温）。
    # 完全性クリティックが「HRV を参照したクエリが無い」と指摘したので追加した。
    # signals_available >= 3 の日だけを対象にする（4指標は睡眠中計測で欠測日が一致する
    # MNAR なので、1つだけ取れた日の値を混ぜると欠測バイアスを増幅する）。
    recovery = TRINO.execute_query(f"""
        SELECT CASE WHEN target_date > DATE '{week_end}' - INTERVAL '7' DAY
                    THEN 'this_week' ELSE 'prev_week' END AS wk,
               count(*) AS n_recovery_days,
               round(avg(hrv_daily_rmssd), 1) AS hrv,
               round(avg(breathing_rate), 1) AS breathing_rate,
               round(avg(spo2_avg), 1) AS spo2,
               round(avg(skin_temp_relative), 2) AS skin_temp_rel
        FROM iceberg.life_silver.fitbit_recovery
        WHERE target_date > DATE '{week_end}' - INTERVAL '14' DAY
          AND target_date <= DATE '{week_end}'
          AND signals_available >= 3
        GROUP BY 1
    """)
    for r in it._records(recovery):
        out.setdefault(r["wk"], {}).update({k: v for k, v in r.items() if k != "wk"})

    out["_note"] = (
        "**各指標の n_* を必ず確認すること。** 欠測がある指標は週ごとに別の部分集合を"
        "平均しているため、n が違う週の平均を直接比較してはいけない。"
        "睡眠の質は deep_min_per_data_day（v1.2 段階データ）を使う。"
        "sleep_h_v1 は restless を含み約15%多いので、絶対値の議論には使わない。"
        "**_per_data_day と _per_calendar_day の両方で同じ方向に動いている変化だけを主張すること。**"
        "片方だけで動いているものは観測日数の差による見せかけ。"
    )
    return out


@task(name="Fetch weekday vs weekend")
def fetch_daytype_comparison(week_end: str, days: int = 28) -> dict:
    """平日 vs 休日の比較。

    ★なぜ28日窓か★
    7日窓では週末が2日しか入らず、平日5日との比較が成立しない。
    28日なら週末8日・平日20日になる。

    ★なぜ必要か★
    これまでの分析は全て「日ごと」または「週ごと」で、**平日と休日を一度も
    分けていなかった**。休日の過ごし方は生活リズムの起点（休日に夜型化すると
    週明けが崩れる）なので、分けないと原因が見えない。
    """
    df = TRINO.execute_query(f"""
        WITH d AS (
            SELECT f.target_date,
                   CASE WHEN day_of_week(f.target_date) >= 6 THEN 'weekend' ELSE 'weekday' END AS daytype,
                   f.total_minutes_asleep, f.resting_heart_rate, f.steps,
                   f.sedentary_minutes, f.lightly_active_minutes,
                   f.fairly_active_minutes, f.very_active_minutes,
                   w.work_core_sec, w.dev_core_sec, w.work_score,
                   a.calories_kcal, a.lunch_calories, a.salt_g, a.fiber_g, a.protein_g,
                   p.unlock_count, p.unlock_count_00_04
            FROM iceberg.life_gold.mrt_fitness_daily_summary f
            LEFT JOIN iceberg.life_gold.mrt_aw_daily_work_summary w ON f.target_date = w.target_date
            LEFT JOIN iceberg.life_gold.mrt_asken a ON f.target_date = a.target_date
            LEFT JOIN iceberg.life_gold.mrt_ai_phone_daily p ON f.target_date = p.target_date
            WHERE f.target_date > DATE '{week_end}' - INTERVAL '{days}' DAY
              AND f.target_date <= DATE '{week_end}'
        ),
        stages AS (
            SELECT stage_date_jst AS d,
                   sum(CASE WHEN is_main_sleep AND stage <> 'wake' AND NOT is_short_wake
                            THEN stage_seconds END)/60.0 AS main_asleep_min,
                   min(CASE WHEN is_main_sleep THEN stage_start_jst END) AS sleep_start
            FROM iceberg.life_silver.fitbit_sleep_stages
            WHERE stage_date_jst > DATE '{week_end}' - INTERVAL '{days}' DAY
            GROUP BY 1
        )
        SELECT d.daytype, count(*) AS n_days,
               round(avg(s.main_asleep_min), 1) AS main_asleep_min,
               count(s.main_asleep_min) AS n_main_sleep,
               -- 就寝時刻。正午起点に直してから平均する（0時をまたぐため）
               round(avg(CASE WHEN s.sleep_start IS NOT NULL
                    THEN (CASE WHEN hour(s.sleep_start) < 12 THEN hour(s.sleep_start) + 24
                               ELSE hour(s.sleep_start) END) + minute(s.sleep_start)/60.0 - 12 END), 2)
                    AS bedtime_hours_after_noon,
               round(avg(d.sedentary_minutes), 0) AS sedentary_min,
               round(avg(d.lightly_active_minutes), 0) AS light_active_min,
               round(avg(d.fairly_active_minutes + d.very_active_minutes), 1) AS mod_vig_active_min,
               round(avg(d.steps), 0) AS steps,
               round(avg(d.work_core_sec)/3600.0, 2) AS work_h,
               round(avg(d.dev_core_sec)/3600.0, 2) AS dev_h,
               round(avg(d.calories_kcal), 0) AS cal_in,
               count_if(d.calories_kcal > 0) AS n_meal_log,
               count_if(d.lunch_calories > 0) AS n_lunch,
               round(avg(d.salt_g), 1) AS salt_g,
               round(avg(d.fiber_g), 1) AS fiber_g,
               round(avg(d.protein_g), 1) AS protein_g,
               round(avg(d.unlock_count), 1) AS unlocks,
               round(avg(d.unlock_count_00_04), 1) AS unlocks_00_04
        FROM d LEFT JOIN stages s ON d.target_date = s.d
        GROUP BY d.daytype
    """)
    out = {r["daytype"]: {k: v for k, v in r.items() if k != "daytype"} for r in it._records(df)}

    # 平日/休日別の cat_sub 上位。過ごし方の中身の違いを見る
    act = TRINO.execute_query(f"""
        SELECT CASE WHEN day_of_week(activity_date_jst) >= 6 THEN 'weekend' ELSE 'weekday' END AS daytype,
               cat_sub,
               round(sum(seconds)/60.0
                     / count(DISTINCT activity_date_jst), 1) AS min_per_day
        FROM iceberg.life_gold.mrt_ai_activity_hourly
        WHERE source = 'window'
          AND activity_date_jst > DATE '{week_end}' - INTERVAL '{days}' DAY
          AND activity_date_jst <= DATE '{week_end}'
        GROUP BY 1, 2
        HAVING sum(seconds) > 3600
        ORDER BY 1, 3 DESC
    """)
    for r in it._records(act):
        out.setdefault(r["daytype"], {}).setdefault("活動_分per日", {})[r["cat_sub"]] = r["min_per_day"]

    out["_note"] = (
        f"直近{days}日を平日/休日で分けた比較（7日窓では週末が2日しかなく比較できないため）。"
        "bedtime_hours_after_noon は正午起点の就寝時刻（例: 14.5 = 深夜2:30）。"
        "**これまでの分析は平日/休日を一度も分けていなかったので、ここは未開拓。**"
        "n_* が小さい指標の差は主張しないこと。"
    )
    return out


@task(name="Fetch intervention effects")
def fetch_intervention_effects(week_end: str) -> list[dict]:
    """介入日で期間を切った前後比較。

    今回の調査で「どれを止めたら効果があったか」が分かった理由がこれ。
    日次FBは「今日 vs 14日平均」しか見ないので介入の効果を測れない。
    """
    out = []
    for v in it.load_interventions(limit_days=60):
        started = (v.get("started_at") or "")[:10]
        if not started:
            continue
        df = TRINO.execute_query(f"""
            WITH w AS (
                SELECT activity_date_jst AS d, cat_main, cat_sub, source, seconds,
                       CASE WHEN activity_date_jst < DATE '{started}' THEN 'before' ELSE 'after' END AS phase
                FROM iceberg.life_gold.mrt_ai_activity_hourly
                WHERE source = 'window'
                  AND activity_date_jst >= DATE '{started}' - INTERVAL '7' DAY
                  AND activity_date_jst <= LEAST(DATE '{started}' + INTERVAL '7' DAY, DATE '{week_end}')
            )
            SELECT cat_main,
                   round(sum(CASE WHEN phase='before' THEN seconds END)/60.0
                         / NULLIF(count(DISTINCT CASE WHEN phase='before' THEN d END), 0), 1) AS before_min_day,
                   round(sum(CASE WHEN phase='after' THEN seconds END)/60.0
                         / NULLIF(count(DISTINCT CASE WHEN phase='after' THEN d END), 0), 1) AS after_min_day
            FROM w GROUP BY cat_main
            HAVING sum(seconds) > 1800
            ORDER BY 2 DESC NULLS LAST
        """)
        out.append({
            "intervention_id": v["intervention_id"],
            "started": started,
            "description": v["description"][:160],
            "cat_main別_前後": it._records(df),
        })
    return out


@task(name="Fetch recent feedback history")
def fetch_recent_feedback(week_end: str, days: int = 14) -> list[dict]:
    df = TRINO.execute_query(f"""
        SELECT CAST(feedback_date AS VARCHAR) AS d, slot, messages
        FROM iceberg.life_gold.ai_feedback
        WHERE feedback_date > DATE '{week_end}' - INTERVAL '{days}' DAY
        ORDER BY feedback_date DESC
    """)
    out = []
    for r in it._records(df):
        try:
            msgs = json.loads(r["messages"])
            out.append({"date": r["d"], "slot": r["slot"],
                        "messages": [m.get("message", "") for m in msgs]})
        except Exception:  # noqa: BLE001
            continue
    return out


@task(name="Build weekly context")
def build_weekly_context(week_end: str) -> dict:
    evaluated = it.evaluate_all_issues(week_end)
    it.enforce_abandonment(evaluated)
    return {
        "week_end": week_end,
        # 機械的な総当たり探索の結果。人が思いついた検出器に依存せず、
        # 関係の型（重なり・遷移・不在）を全部試したもの。
        "structural_discovery": discovery.run_discovery(week_end),
        "week_over_week": fetch_weekly_comparison(week_end),
        # 平日/休日の比較。これまで一度も分けていなかった領域。
        "weekday_vs_weekend": fetch_daytype_comparison(week_end),
        "intervention_effects": fetch_intervention_effects(week_end),
        "active_issues": [
            {k: e[k] for k in (
                "issue_id", "title", "hypothesis", "status", "metric_name",
                "baseline_value", "current_value", "target_value", "target_direction",
                "change_from_baseline_pct", "is_moving", "mention_count", "eval_error",
            )} | {"history": [
                {"d": h["eval_date"], "v": h["metric_value"]}
                for h in (e.get("history") or []) if h.get("metric_value") is not None
            ]}
            for e in evaluated if e["status"] in ("open", "testing")
        ],
        "recent_feedback": fetch_recent_feedback(week_end),
    }


# ─────────────────────────────────────────────────────────────
# 生成（SQL 探索 + 検索つき）
# ─────────────────────────────────────────────────────────────
_RUN_SQL_DECL = types.FunctionDeclaration(
    name="run_sql",
    description=(
        "Trino に読み取り専用SQLを投げて結果をMarkdown表で受け取る。"
        "仮説を検証したいとき、渡されたコンテキストに無い切り口を試したいときに使う。"
    ),
    parameters=types.Schema(
        type=types.Type.OBJECT,
        properties={"query": types.Schema(type=types.Type.STRING, description="SELECT または WITH で始まる1文")},
        required=["query"],
    ),
)

_WEEKLY_PROMPT = """\
あなたはユーザー専属のライフアナリストです。週に1回、月曜の朝に届く分析を書きます。

## あなたの役割

日次フィードバックは「進行中の実験の実況」に徹しています。
**洞察を出すのも、今週の実験を決めるのも、あなただけの仕事です。**

## 使える道具

`run_sql` で Trino に読み取り専用SQLを投げられます。渡されたコンテキストは出発点に
過ぎません。**仮説を思いついたら必ずSQLで検証してください。** 最低5回は掘ること。
Web検索も使えますが、**順序を守ってください**: 先に自分のデータの数値を確定させ、
それを説明するために文献知識を引く。逆順にすると一般論に戻ります。

{schema_doc}

## 探索の指針（何を探すか）

要約統計は本人に情報を与えません。その日を生きたのは本人です。
情報になるのは次の4種類だけです:

1. **本人が計算できないもの** — 複数系列の関係、遅延効果、条件付き効果
2. **本人の予測を裏切るもの** — 「思っていたのと逆」
3. **反事実** — 「もし止めていたら」
4. **外部知識との接続** — 自分の数値に文献の知見を当てる

### `structural_discovery` の扱い（重要）

重なり・遷移・不在の総当たりは**すでに機械的に実行済み**で、結果が渡されています。
**BH-FDR を通過した候補だけが載っています。再検定は不要です。**

- `overlaps.candidates` … 同時刻に立つ活動の組み合わせ。
  `ratio` は「同じ日の中で独立なら」との比、`ratio_within_hour` は
  「同じ時間帯の中で独立なら」というより厳しい基準との比です。
  **`ratio_within_hour` が 1 付近なら、それは単に『同じ時間帯に起きている』だけ**で、
  行動として結びついている証拠にはなりません。1を大きく超えるものだけが強い所見です。
- `transitions` … 「出来事の直前に何があったか」。**`summary.negative_result` が
  入っている場合、検証した結果として何も無かったという意味です。**
  その場合「〜の直前に〜していた」と書いてはいけません（時間帯の偏りを因果と誤読します）。
- `absences.candidates` … 「何が起きなかったか」。`interpretation_note` を見て
  **計測の欠測**と**行動の不在**を区別してください。欠測を「改善」と読まないこと。

**これらに載っていない重なり・遷移を SQL で見つけたと主張する場合は、
時間帯の交絡を自分で除いたことを示してください。**

### これまで手つかずの領域（優先的に見る価値がある）

- **`weekday_vs_weekend`**: これまでの分析は平日/休日を一度も分けていません。
  休日の就寝が遅れると週明けが崩れる（社会的ジェットラグ）ため、生活リズムの起点として重要です。
- **栄養の詳細**: `salt_g` / `fat_g` / `carbs_g` / `fiber_g` / `protein_g` が渡されています。
  以前はカロリーとタンパク質しか見ていませんでした。
- **活動強度**: `sedentary_min` / `light_active_min` / `mod_vig_active_min`。
  以前は very_active しか見ておらず、座位時間（1日の大半）を見落としていました。

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

### 本人の第一目標（変更不可の前提として扱う）

**業務時間・仕事のパフォーマンスの向上が最優先の目標。** 本人の言葉:
「仕事をサボってしまうと自分を責めて余計苦しくなる」。
したがって「work_score は計測が弱いから目標にしない」という助言をしてはいけない。
測れないなら測り方を提案する側に回ること。

関連する既知の事実（issue ISS-328F0FF9）:
- YouTubeブロックで MEDIA は 441→260分/日 に減ったが **WORK は 106→103分/日で不変**。
  空いた時間は DEVELOP(+137) と BROWSING(+99) に流れた。
  「YouTubeが業務時間を奪っていた」という前提は成立していない。
- 睡眠・深夜画面・解錠・食事・座位・心拍のいずれも work_score と有意な相関なし（11項目0件通過）。
  ただし n が小さく、work_core_sec 自体が弱い指標（会議・読む・考えるを計上しない）。

### カテゴリ分類の限界（2026-08-29 時点）

`BROWSING / ネットサーフィン` は**中身が保証されない箱**。本人の申告で、
応用情報の過去問道場（学習）や入眠用の動画が混ざっている。
Android のブラウザは前面イベントにタイトルが付かず（Firefox 647分 / Chrome 323分が
タイトル無し）、**何を見ていたか分からない**。
BROWSING の増減を「逃避が増えた/減った」と解釈してはいけない。

## 統計の扱い（厳守）

- **`week_over_week` の n_* を必ず確認する。** 欠測がある指標は週ごとに別の部分集合を
  平均しているので、n が違う週の平均を「変化した」と読んではいけない。
  実害の例: 深睡眠が v1 由来で 97.3分(n=4) → 55.6分(n=5) と出て「半減した」と
  結論されたが、v1.2 の段階データでは 61.3分 → 61.2分 で**変化していなかった**。
  睡眠の質は `deep_min_per_data_day`（v1.2）を使うこと。
- **`_per_data_day` と `_per_calendar_day` の両方で同じ方向に動いている変化だけを主張する。**
  片方だけで動いているものは観測日数の差（例: prev 5日 / this 7日）による見せかけ。
- 相関を主張するなら **n を必ず添える**。n<10 では相関を主張しないこと
- 14日窓なら n は最大13。その場合 |r| >= 0.55 でないと偶然と区別できない
- 多数の組み合わせを試すほど偽陽性が増える。**5件の候補から1件を選ぶなら、
  なぜそれが偶然でないかを述べること**
- 記述統計を因果として書かない

## 今週の実験（最重要・必ず1つだけ）

分析の最後に、**今週試す実験を1つだけ**決めてください。

必須条件:
- **`metric_sql` が書けること。** 既存テーブルのカラムと閾値で自動評価できない提案は
  出力してはいけません（「十分な睡眠を」「無理は禁物」は metric を書けないので却下）
- `metric_sql` は **1行1列の数値** を返し、日付は `{{eval_date}}` プレースホルダで受ける
  （`current_date` は使わない。セッションTZで日付がずれる）
- **`baseline_value` と `metric_sql` の集計基準を必ず揃えること。**
  日々のノイズを避けるため **7日平均**を推奨する。例:
  `... WHERE d BETWEEN DATE '{{eval_date}}' - INTERVAL '6' DAY AND DATE '{{eval_date}}'` の合計を 7.0 で割る。
  baseline を7日平均で出したのに metric_sql が単日を返すと、進捗がノイズで誤読される
- **閲覧先の実体（ドメイン名）は参照できない。** 逃避先が移動したことは
  cat_main / cat_sub の増減で述べれば十分で、移動先を名指しする必要はない
- **症状ではなく原因に介入する。** 例: 主睡眠が短いから昼寝で補填している場合、
  昼寝を削ると総睡眠が減るだけで悪化する。手を入れるのは深夜側
- 既存の `active_issues` と重複しないこと。既存のものが停滞しているなら、
  同じ課題に**別の角度の仮説**を立てるのは可

## 出力形式（JSONのみ・他のテキスト一切不要）

{{
  "insights": [
    {{"type": "insight"|"warning"|"danger"|"positive",
      "message": "200文字以内。数値の根拠を含める",
      "issue_ids": ["関連する既存issue_id（無ければ空配列）"]}}
  ],
  "experiment": {{
    "title": "40文字以内",
    "hypothesis": "原因の仮説。症状ではなく原因を書く",
    "metric_sql": "SELECT ... WHERE ... {{eval_date}} ...",
    "metric_name": "指標の名前",
    "metric_unit": "分 / 回 / 時間 など",
    "baseline_value": 数値,
    "target_value": 数値,
    "target_direction": "decrease"|"increase",
    "why_not_chance": "なぜこれが偶然の発見でないか"
  }},
  "queries_run": ["実際に投げた主要なSQLを3〜5本"]
}}

insights は3〜5件。既に日次で伝えた内容（`recent_feedback`）の繰り返しは禁止です。

## データ
{data}
"""

_CRITIC_PROMPT = """\
あなたは分析の完全性を監査する役です。以下は今週行われた分析です。

**あなたの仕事は「何を見落としたか」を挙げることだけです。** 分析の良い点は書かないでください。

観点:
- 一度も参照されなかったデータソース・テーブルはどれか
- 検証されないまま述べられた主張はあるか
- 一度も言及されなかった活動カテゴリ・時間帯はあるか
- 「不在」（記録が無い日、消えた活動）を確認したか
- 統計的に弱い根拠で述べられたものはあるか

## 今週の分析
{analysis}

## 実行されたSQL
{queries}

## 既に利用できるデータ（これらは「計装の提案」にしてはいけない）
{schema_doc}

## 過去の計装提案（重複を避けるために必ず確認する）
{past_proposals}

**同じ問いが既に提案されている場合は、新規に立てずに `existing_proposal_id` に
その ID を入れてください。** 文言が違っても問いが同じなら同一として扱います。
`status` が `implemented` / `already_satisfied` のものは**再提案しないこと**。

## もう1つの仕事: 「今のデータでは答えられなかった問い」を挙げる

分析中に「これが分かれば説明できたのに、データが無い」と詰まった箇所を挙げてください。
これは**新しく計測を始めるべきものの候補**になります。

★厳守すべき制約★
- **手入力を必要とする計装は提案してはいけない。**
  気分の5段階入力のようなものは、調子が悪い日ほど記録が飛ぶ（欠測がランダムでない）ため、
  最も欲しいデータだけが構造的に欠けます。実際この理由で一度却下されています。
  提案するのは「本人が何もしなくても溜まるもの」だけです。
- すでに取れているのに使っていないだけのものは「計装の提案」ではなく `missed` に書く。
  例: 塩分・座位時間・平日休日の別・栄養素の詳細は**すでにテーブルにあります**。
- 各提案に「それで何が判定できるようになるか」を必ず書く。
  判定できることが書けないものは提案しない。

## 出力形式（JSONのみ）
{{
  "missed": ["見落とし1", "見落とし2", ...],
  "next_week_targets": ["翌週に調べるべきこと", ...],
  "instrumentation_gaps": [
    {{"question": "答えられなかった問い",
      "missing_data": "何が無いのか",
      "how_to_collect_passively": "本人の手を借りずに取る方法（無理なら『受動的手段なし』と書く）",
      "would_enable": "取れたら何が判定できるようになるか",
      "existing_proposal_id": "同じ問いが過去に提案済みならその ID。新規なら空文字"}}
  ]
}}

missed は3〜7件、instrumentation_gaps は0〜4件。翌週の探索対象になるので具体的に。
"""


def _extract_json(text: str):
    text = text.strip()
    if "```" in text:
        parts = text.split("```")
        text = parts[1] if len(parts) > 1 else parts[0]
        if text.startswith("json"):
            text = text[4:]
    return json.loads(text.strip())


@task(name="Generate weekly analysis", retries=1, retry_delay_seconds=60)
def generate_weekly(ctx: dict, api_key: str) -> dict:
    client = genai.Client(api_key=api_key)
    prompt = _WEEKLY_PROMPT.format(
        schema_doc=sql_tool.SCHEMA_DOC,
        data=json.dumps(ctx, ensure_ascii=False, indent=2, default=str),
    )

    tools = [types.Tool(function_declarations=[_RUN_SQL_DECL]), types.Tool(google_search=types.GoogleSearch())]
    config = types.GenerateContentConfig(
        tools=tools,
        # 組み込みツール（google_search）と自前の関数呼び出しを併用するには
        # これが必須。無いと 400 INVALID_ARGUMENT
        # 「Please enable tool_config.include_server_side_tool_invocations」で落ちる。
        tool_config=types.ToolConfig(include_server_side_tool_invocations=True),
        thinking_config=types.ThinkingConfig(thinking_budget=THINKING_BUDGET),
    )
    contents = [types.Content(role="user", parts=[types.Part(text=prompt)])]
    executed: list[str] = []

    for turn in range(MAX_TOOL_TURNS):
        resp = client.models.generate_content(model=MODEL, contents=contents, config=config)
        cand = resp.candidates[0] if resp.candidates else None
        parts = (cand.content.parts if cand and cand.content else None) or []
        calls = [p.function_call for p in parts if getattr(p, "function_call", None)]

        if not calls:
            text = "".join(p.text for p in parts if getattr(p, "text", None))
            print(f"🔎 SQL探索 {len(executed)}回で分析完了（{turn + 1}往復）")
            result = _extract_json(text)
            result["_queries_executed"] = executed
            return result

        contents.append(cand.content)
        for call in calls:
            query = (call.args or {}).get("query", "")
            try:
                out = sql_tool.run_sql(query)
                executed.append(query)
                print(f"   SQL[{len(executed)}]: {query[:110]}")
            except sql_tool.SqlToolError as e:
                out = f"REJECTED: {e}"
                print(f"   SQL拒否: {e}")
            contents.append(types.Content(
                role="user",
                parts=[types.Part.from_function_response(name="run_sql", response={"result": out})],
            ))

    raise RuntimeError(f"SQL探索が {MAX_TOOL_TURNS} 往復で収束しなかった")


@task(name="Run completeness critic", retries=1, retry_delay_seconds=30)
def run_critic(analysis: dict, api_key: str) -> dict:
    """「何を見落としたか」だけを問う枠。ここで出たものが翌週の探索対象になる。"""
    client = genai.Client(api_key=api_key)
    try:
        resp = client.models.generate_content(
            model=MODEL,
            contents=_CRITIC_PROMPT.format(
                analysis=json.dumps(
                    {k: v for k, v in analysis.items() if not k.startswith("_")},
                    ensure_ascii=False, indent=2),
                queries=json.dumps(analysis.get("_queries_executed") or [], ensure_ascii=False, indent=2),
                schema_doc=sql_tool.SCHEMA_DOC,
                past_proposals=json.dumps(
                    it.load_instrumentation_proposals(), ensure_ascii=False, indent=2, default=str),
            ),
            config=types.GenerateContentConfig(
                thinking_config=types.ThinkingConfig(thinking_budget=4096),
            ),
        )
        return _extract_json(resp.text)
    except Exception as e:  # noqa: BLE001
        print(f"⚠️ 完全性クリティックに失敗（分析自体は保存する）: {e}")
        return {"missed": [], "next_week_targets": [], "error": str(e)[:200]}


@task(name="Register weekly experiment")
def register_experiment(exp: dict, week_end: str) -> str | None:
    """今週の実験を issue として登録する。metric_sql を書けていなければ登録しない。"""
    if not exp:
        print("⚠️ experiment が空。今週の実験は登録しない")
        return None
    try:
        issue_id = it.create_issue(
            title=exp["title"],
            hypothesis=exp["hypothesis"],
            discovered_by="weekly_llm",
            evidence={"why_not_chance": exp.get("why_not_chance")},
            metric_sql=exp["metric_sql"],
            metric_name=exp["metric_name"],
            metric_unit=exp.get("metric_unit") or "",
            baseline_value=float(exp["baseline_value"]),
            target_value=float(exp["target_value"]),
            target_direction=exp["target_direction"],
            opened_date=week_end,
            status="open",
        )
    except (KeyError, ValueError, TypeError) as e:
        # metric_sql が無い・方向が不正などは「処方として不成立」なので登録しない。
        # ここで弾くことが「検証できない助言」を機械的に排除する仕掛け。
        print(f"❌ 今週の実験を登録できなかった（metric の条件を満たしていない）: {e}")
        return None

    # metric_sql が実際に動くかを登録直後に確かめる。動かない metric は
    # 効果測定ループを黙って殺すので、ここで検出して notes に残す。
    value, err = it.evaluate_metric({"metric_sql": exp["metric_sql"]}, week_end)
    if err:
        it.set_status(issue_id, "open", f"[warn] metric_sql の初回評価が失敗: {err}")
        print(f"⚠️ {issue_id} の metric_sql が評価できない: {err}")
        return issue_id

    it.record_metric(issue_id, week_end, value)
    print(f"✅ 今週の実験を登録: {issue_id} 「{exp['title']}」 metric初期値={value}")

    # LLM が申告した baseline と metric_sql の実測値がズレていないか照合する。
    # ズレる典型は「baseline は7日平均で出したが metric_sql は単日を返す」という
    # 集計基準の不一致で、この場合 progress が日々のノイズで誤読される。
    # 実際に 2026-08-27 の初回生成で baseline=223.0（7日平均）に対し
    # metric_sql が単日を返し 226.0 になっていた。
    stated = float(exp["baseline_value"])
    if value and abs(stated - value) / abs(value) > 0.10:
        warn = (
            f"[warn] 申告 baseline {stated} と metric_sql の実測 {value} が10%以上乖離。"
            "集計基準（単日 / N日平均）が食い違っている可能性がある。"
            f"baseline を実測値 {value} に置き換えた。"
        )
        it.TRINO.execute_action(f"""
            UPDATE iceberg.life_gold.ai_feedback_issues
            SET baseline_value = {value},
                notes = COALESCE(notes, '') || {it._q(warn)},
                updated_at = CURRENT_TIMESTAMP
            WHERE issue_id = {it._q(issue_id)}
        """)
        print(f"⚠️ {warn}")
    return issue_id


@task(name="Save weekly feedback")
def save_weekly(week_end: str, analysis: dict, critic: dict, ctx: dict) -> None:
    import pandas as pd

    now_jst = datetime.datetime.now(JST).replace(tzinfo=None)
    messages = analysis.get("insights") or []
    TRINO.execute_action(
        f"DELETE FROM iceberg.life_gold.ai_feedback "
        f"WHERE feedback_date = DATE '{week_end}' AND slot = '{SLOT}'"
    )
    df = pd.DataFrame([{
        "feedback_date": datetime.date.fromisoformat(week_end),
        "slot": SLOT,
        "generated_at": now_jst,
        "messages": json.dumps(messages, ensure_ascii=False),
        "model": MODEL,
        "context_summary": json.dumps(
            {"experiment": analysis.get("experiment"),
             "critic": critic,
             "queries_executed": analysis.get("_queries_executed"),
             "context": ctx},
            ensure_ascii=False, default=str),
    }])
    TRINO.insert_table("ai_feedback", "life_gold", df)
    print(f"✅ 週次FBを保存: {week_end} [{SLOT}] insights={len(messages)}")


@flow(name="AI Feedback Weekly", log_prints=True)
def ai_feedback_weekly_flow(week_end: str | None = None, force: bool = False):
    """週次分析。月曜朝に前週（日曜まで）を対象に走らせる。"""
    now_jst = datetime.datetime.now(JST)
    if week_end is None:
        # 月曜に走るので、対象期間の末尾は前日（日曜）
        week_end = (now_jst - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

    if not force:
        df = TRINO.execute_query(f"""
            SELECT count(*) AS n FROM iceberg.life_gold.ai_feedback
            WHERE feedback_date = DATE '{week_end}' AND slot = '{SLOT}'
        """)
        if int(df.iloc[0]["n"]) > 0:
            print(f"⏭  {week_end} の週次FBは既に生成済み。スキップ")
            return {"skipped": True}

    print(f"🤖 週次FB生成: week_end={week_end} model={MODEL}")
    api_key = Secret.load("google-generative-ai-api-key").get()

    ctx = build_weekly_context(week_end)
    print(f"   進行中 issue: {len(ctx['active_issues'])}件 / 介入: {len(ctx['intervention_effects'])}件")

    analysis = generate_weekly(ctx, api_key)
    critic = run_critic(analysis, api_key)
    # 計装提案を追跡可能な形で保存する。context_summary に埋めるだけでは
    # 同じ提案が毎週出ても気づけない（実際に既に実装済みの HRV が再提案された）。
    proposal_result = it.record_instrumentation_proposals(
        critic.get("instrumentation_gaps") or [], week_end
    )
    if proposal_result["new"] or proposal_result["bumped"] or proposal_result["skipped"]:
        print(
            f"   計装提案: 新規 {len(proposal_result['new'])}件 / "
            f"再提案 {len(proposal_result['bumped'])}件 / "
            f"不採用 {len(proposal_result['skipped'])}件"
        )
    issue_id = register_experiment(analysis.get("experiment") or {}, week_end)
    save_weekly(week_end, analysis, critic, ctx)

    return {
        "skipped": False,
        "insights": len(analysis.get("insights") or []),
        "queries_executed": len(analysis.get("_queries_executed") or []),
        "experiment_issue_id": issue_id,
        "missed_by_critic": len(critic.get("missed") or []),
        "instrumentation_proposals": proposal_result,
    }


if __name__ == "__main__":
    ai_feedback_weekly_flow(force=True)
