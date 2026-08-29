"""構造的総当たり発見エンジン。

なぜ必要か:
  従来の検出器はハードコードだった（`sleep_behavior_signals` の
  afternoon_nap / late_night_screen / past_midnight_activity の3種）。
  「誰かが思いついた3つ」しか見つけられないので、4つ目を永久に見逃す。

  実際、寝落ち視聴は**統計的な異常ではなく表現の問題**だった
  （旧 mrt_behavior_slots_15m は priority で SLEEP が MEDIA を上書きしていたので
  データに存在しなかった）。異常検知をいくら回しても見つからない。
  一方、週次FBが SQL 探索で「長い昼寝の直前に必ずブラウジングが先行する」を
  見つけたのは偶然に依存している。

  そこで**関係の型を機械的に全部試す**層をここに置く。人の思いつきに依存させない。

3つの型:
  1. 重なり(overlap)   … 同時刻に立つ活動の組み合わせを総当たり
  2. 遷移(transition)  … ある出来事の「直前」に何が起きているか
  3. 不在(absence)     … 期待される記録が無い日／消えた活動

いずれも候補を出すだけで、断定はしない。多重検定は stats.benjamini_hochberg で
制御し、**生き残った候補だけを LLM に渡す**（LLM に検定させない）。
"""

from prefect import task

from ai_feedback import issue_tracker as it
from ai_feedback import stats
from common.trino_api import TrinoAPI

TRINO = TrinoAPI(host="trino.mynet", port=80, user="ai-discovery", catalog="iceberg")

# 探索窓。長すぎると生活の変化を混ぜ込み、短すぎると検出力が出ない。
LOOKBACK_DAYS = 28

# この分数未満の重なりは正規近似が粗くなるので検定にかけない
MIN_OVERLAP_MINUTES = 30

# 遷移の「直前」をどれだけ遡るか
TRANSITION_LOOKBEHIND_MIN = 120


# ─────────────────────────────────────────────────────────────
# 1. 重なり
# ─────────────────────────────────────────────────────────────
# ★ドメインの切り方が結果を決める★
# 最初は window / web / media を別ソースとして総当たりしたが、上位が
#   web:BROWSING × window:BROWSING（同じブラウジングを2つのウォッチャーが捉えた）
#   media:MEDIA × window:MEDIA（同じ動画の前面時間と再生時間）
# という**計測アーティファクト**で埋まった。同じ行動を二重に観測しているだけで、
# 行動の知見がゼロ。
#
# そこで画面系3つは 'screen' に畳み（cat_sub は保持）、**物理的に独立なドメイン間**
# だけを比べる:
#   screen … 画面に向かっていた（window + web + media を分単位で重複排除）
#   sleep  … 寝ていた（Fitbit）
#   hr_gap … 心拍が途切れていた＝時計を外していた（入浴など）
# これで「睡眠 × YouTube（寝落ち）」「入浴 × 画面」のような、
# 異なる領域の重なりだけが残る。
_OVERLAP_SQL = """
WITH ev AS (
    SELECT 'screen' AS src, cat_sub, start_ts, end_ts
    FROM iceberg.life_gold.int_aw_categorized
    WHERE NOT is_afk AND event_date_jst >= DATE '{start}' AND end_ts > start_ts
    UNION ALL
    SELECT 'screen', cat_sub, start_ts, end_ts
    FROM iceberg.life_gold.int_aw_media
    WHERE event_date_jst >= DATE '{start}' AND end_ts > start_ts
    UNION ALL
    SELECT 'screen', cat_sub, start_ts, end_ts
    FROM iceberg.life_gold.int_aw_web
    WHERE event_date_jst >= DATE '{start}' AND end_ts > start_ts
    UNION ALL
    SELECT 'sleep', cat_sub, start_ts, end_ts
    FROM iceberg.life_gold.int_fitbit_sleep
    WHERE event_date_jst >= DATE '{start}' AND end_ts > start_ts
    UNION ALL
    -- 心拍の欠測区間。時計を外していた時間で、入浴として分類されている
    SELECT 'hr_gap', cat_sub, start_ts, end_ts
    FROM iceberg.life_gold.int_fitbit_hr_gaps
    WHERE event_date_jst >= DATE '{start}' AND end_ts > start_ts
),
-- 分単位に展開。同じ src×cat_sub の重複はここで潰れる
-- （= window と media が同じ YouTube を報告しても1分は1分）。
mins AS (
    SELECT DISTINCT src, cat_sub, date_trunc('minute', m) AS mm
    FROM ev
    CROSS JOIN UNNEST(sequence(
        date_trunc('minute', start_ts), date_trunc('minute', end_ts), INTERVAL '1' MINUTE
    )) AS t(m)
    WHERE m < end_ts
),
per_hour AS (
    SELECT src, cat_sub, CAST(mm AS DATE) AS d, hour(mm) AS h, count(*) AS mins_in_hour
    FROM mins GROUP BY 1, 2, 3, 4
),
-- 観測された重なり。**src が異なるものだけ**（同一 src 内は同じ領域なので無意味）
observed AS (
    SELECT a.src AS src_a, a.cat_sub AS cat_a, b.src AS src_b, b.cat_sub AS cat_b,
           count(*) AS overlap_min
    FROM mins a
    JOIN mins b ON a.mm = b.mm AND a.src < b.src
    GROUP BY 1, 2, 3, 4
),
-- ★帰無仮説を2つ持つ理由★
-- 当初は「同じ日・同じ時間帯での独立」だけを期待値にしたが、それでは
-- 検出したい現象そのものを条件付けで消してしまう。実測で
--   screen:YouTube × sleep:主睡眠 = 観測1031分 / 期待1069.8分 / 比0.96
-- となり「重なりは偶然の範囲」と出た。原因は、その時間帯にほぼ寝ているなら
-- 画面時間は当然睡眠と重なる、という自明な計算になっていたこと。
-- 「画面時間が睡眠帯に集中している」ことこそが現象なのに、時間帯で条件付けると
-- それが前提に繰り込まれる。
--
-- そこで2つ並べる:
--   expected_day  … 同じ日の中での独立。「1日のうちいつ画面を見るか」が
--                    睡眠帯に偏っているかを検出する（＝現象そのもの）
--   expected_hour … 同じ日・同じ時間帯での独立。時間帯の偏りを超えて
--                    さらに集中しているかを見る、より厳しい基準
-- 主判定は day、hour は「時間帯の偏りだけでは説明できない」ことの補強に使う。
per_day AS (
    SELECT src, cat_sub, CAST(mm AS DATE) AS d, count(*) AS mins_in_day
    FROM mins GROUP BY 1, 2, 3
),
expected_day AS (
    SELECT pa.src AS src_a, pa.cat_sub AS cat_a, pb.src AS src_b, pb.cat_sub AS cat_b,
           sum(CAST(pa.mins_in_day AS DOUBLE) * pb.mins_in_day / 1440.0) AS expected_min
    FROM per_day pa
    JOIN per_day pb ON pa.d = pb.d AND pa.src < pb.src
    GROUP BY 1, 2, 3, 4
),
expected_hour AS (
    SELECT pa.src AS src_a, pa.cat_sub AS cat_a, pb.src AS src_b, pb.cat_sub AS cat_b,
           sum(CAST(pa.mins_in_hour AS DOUBLE) * pb.mins_in_hour / 60.0) AS expected_min
    FROM per_hour pa
    JOIN per_hour pb ON pa.d = pb.d AND pa.h = pb.h AND pa.src < pb.src
    GROUP BY 1, 2, 3, 4
)
SELECT o.src_a, o.cat_a, o.src_b, o.cat_b,
       o.overlap_min,
       round(ed.expected_min, 1) AS expected_day_min,
       round(eh.expected_min, 1) AS expected_hour_min
FROM observed o
JOIN expected_day ed
  ON o.src_a = ed.src_a AND o.cat_a = ed.cat_a AND o.src_b = ed.src_b AND o.cat_b = ed.cat_b
LEFT JOIN expected_hour eh
  ON o.src_a = eh.src_a AND o.cat_a = eh.cat_a AND o.src_b = eh.src_b AND o.cat_b = eh.cat_b
WHERE o.overlap_min >= {min_overlap}
ORDER BY o.overlap_min DESC
"""


@task(name="Discover overlaps", retries=1, retry_delay_seconds=30)
def discover_overlaps(start: str) -> dict:
    """同時刻に立つ活動の組み合わせを総当たりする。

    寝落ち視聴（SLEEP × MEDIA）はこの型の一例に過ぎない。
    食事×WORK、WORK×MANGA、入浴×MEDIA なども同じ枠で自動的に出る。

    期待値は「同じ日・同じ時間帯での独立」を仮定して計算する。
    これをやらないと「両方とも午後に多い」だけで重なって見える。
    """
    df = TRINO.execute_query(_OVERLAP_SQL.format(start=start, min_overlap=MIN_OVERLAP_MINUTES))
    rows = it._records(df)
    candidates = []
    for r in rows:
        obs = r["overlap_min"]
        exp_day, exp_hour = r["expected_day_min"], r["expected_hour_min"]
        p_day = stats.poisson_like_p(obs, exp_day)
        if p_day is None:
            continue
        p_hour = stats.poisson_like_p(obs, exp_hour) if exp_hour else None
        candidates.append({
            "kind": "overlap",
            "a": f"{r['src_a']}:{r['cat_a']}",
            "b": f"{r['src_b']}:{r['cat_b']}",
            "domains": f"{r['src_a']}×{r['src_b']}",
            "observed_min": obs,
            "expected_min": exp_day,
            "ratio": round(obs / exp_day, 2) if exp_day else None,
            # 時間帯の偏りだけでは説明できないか。1を超えていれば
            # 「その時間帯にいること」を差し引いてもなお集中している。
            "ratio_within_hour": round(obs / exp_hour, 2) if exp_hour else None,
            "p_within_hour": p_hour,
            "p": p_day,
        })
    survivors, summary = stats.benjamini_hochberg(candidates)
    # 期待より「多い」方だけを問題として扱う（少ないのは問題ではない）
    survivors = [s for s in survivors if (s.get("ratio") or 0) > 1.0]
    summary["negative_result"] = None if survivors else (
        f"{summary['tested']}件の組み合わせを検定したが、期待を超える重なりは1件も無かった。"
        "同時に起きている活動の組み合わせに異常は無い。"
    )
    summary["null_hypothesis_note"] = (
        "ratio は『同じ日の中で独立なら』という期待値との比。"
        "ratio_within_hour は『同じ時間帯の中で独立なら』というより厳しい基準との比で、"
        "これが1を大きく超えるものは時間帯の偏りだけでは説明できない集中を意味する。"
    )
    return {"candidates": survivors, "summary": summary}


# ─────────────────────────────────────────────────────────────
# 2. 遷移
# ─────────────────────────────────────────────────────────────
_TRANSITION_SQL = """
WITH targets AS (
    -- 対象の出来事: 60分以上の昼寝
    SELECT start_ts AS event_ts, '長い昼寝' AS event_kind
    FROM iceberg.life_gold.int_fitbit_sleep
    WHERE cat_sub = '昼寝' AND event_date_jst >= DATE '{start}'
      AND date_diff('minute', start_ts, end_ts) >= 60
    UNION ALL
    -- 対象の出来事: 主睡眠の開始（＝就寝）
    SELECT start_ts, '就寝'
    FROM iceberg.life_gold.int_fitbit_sleep
    WHERE cat_sub = '主睡眠' AND event_date_jst >= DATE '{start}'
),
acts AS (
    SELECT cat_main, cat_sub, start_ts, end_ts
    FROM iceberg.life_gold.int_aw_categorized
    WHERE NOT is_afk AND event_date_jst >= DATE '{start}' AND end_ts > start_ts
),
-- 活動を分単位に展開しておく（重なりの二重計上を避けつつ、窓の切り出しを容易にする）
act_mins AS (
    SELECT DISTINCT cat_sub, date_trunc('minute', m) AS mm
    FROM acts
    CROSS JOIN UNNEST(sequence(
        date_trunc('minute', start_ts), date_trunc('minute', end_ts), INTERVAL '1' MINUTE
    )) AS t(m)
    WHERE m < end_ts
),
-- 出来事の直前 N 分に何が何分あったか
before AS (
    SELECT t.event_kind, t.event_ts, a.cat_sub, count(*) / 1.0 AS minutes
    FROM targets t
    JOIN act_mins a
      ON a.mm >= t.event_ts - INTERVAL '{lookbehind}' MINUTE AND a.mm < t.event_ts
    GROUP BY 1, 2, 3
),
-- ★ベースラインの時間帯を観測窓と揃える★
-- 当初はイベント時刻の「その時間帯」の平均を基準にしていたが、観測しているのは
-- 直前2時間（＝別の時間帯）なので、比較の土台がずれていた。
-- 実測で14件がFDRを通過したのに全て比<=1になり、表示ゼロという症状で発覚。
--
-- ここでは各イベントの直前窓が実際にかかった hour-of-day を列挙し、
-- その時間帯の平均分数を足し上げて基準にする。
-- これで「昼寝は午後に多い→午後に多い活動が先行して見える」交絡を正しく除ける。
hour_avg AS (
    SELECT h, cat_sub, sum(mins) / CAST(count(DISTINCT d) AS DOUBLE) AS avg_min
    FROM (
        SELECT hour(mm) AS h, CAST(mm AS DATE) AS d, cat_sub, count(*) AS mins
        FROM act_mins GROUP BY 1, 2, 3
    )
    GROUP BY 1, 2
),
-- 各イベントの直前窓が覆う分を hour-of-day 別に数える
window_hours AS (
    SELECT t.event_kind, t.event_ts, hour(w.mm) AS h, count(*) AS window_mins
    FROM targets t
    CROSS JOIN UNNEST(sequence(
        t.event_ts - INTERVAL '{lookbehind}' MINUTE, t.event_ts - INTERVAL '1' MINUTE,
        INTERVAL '1' MINUTE
    )) AS w(mm)
    GROUP BY 1, 2, 3
),
per_event_baseline AS (
    SELECT wh.event_kind, wh.event_ts, ha.cat_sub,
           sum(ha.avg_min * wh.window_mins / 60.0) AS baseline_min
    FROM window_hours wh
    JOIN hour_avg ha ON ha.h = wh.h
    GROUP BY 1, 2, 3
)
SELECT b.event_kind, b.cat_sub,
       count(*) AS n_events,
       round(avg(b.minutes), 1) AS avg_before_min,
       round(avg(pb.baseline_min), 1) AS baseline_min,
       round(stddev(b.minutes), 2) AS sd_before
FROM before b
JOIN per_event_baseline pb
  ON pb.event_kind = b.event_kind AND pb.event_ts = b.event_ts AND pb.cat_sub = b.cat_sub
GROUP BY 1, 2
HAVING count(*) >= 4
ORDER BY 4 DESC
"""


@task(name="Discover transitions", retries=1, retry_delay_seconds=30)
def discover_transitions(start: str) -> dict:
    """「Xの直前に何が起きているか」を総当たりする。

    週次FBが手作業で見つけた「長い昼寝の直前にブラウジングが先行する」を
    人の思いつきに依存せず出すのが狙い。
    時間帯を揃えたベースラインと比べることで、「両方とも午後に多い」だけの
    見せかけの先行を除く。
    """
    df = TRINO.execute_query(
        _TRANSITION_SQL.format(start=start, lookbehind=TRANSITION_LOOKBEHIND_MIN)
    )
    candidates = []
    for r in it._records(df):
        n = r["n_events"]
        obs, base, sd = r["avg_before_min"], r["baseline_min"], r["sd_before"]
        if not base or base <= 0 or n < 4 or sd in (None, 0):
            continue
        # 1標本 t 検定: 直前の平均が同時間帯のベースラインより多いか
        se = sd / (n ** 0.5)
        if se <= 0:
            continue
        t = (obs - base) / se
        p = 2.0 * stats._t_sf(abs(t), n - 1)
        candidates.append({
            "kind": "transition",
            "event": r["event_kind"],
            "preceded_by": r["cat_sub"],
            "n_events": n,
            "avg_before_min": obs,
            "baseline_min": base,
            "ratio": round(obs / base, 2),
            "p": p,
        })
    survivors, summary = stats.benjamini_hochberg(candidates)
    elevated = [s for s in survivors if s["ratio"] > 1.0]
    # ★「検証したが何も無かった」を明示する★
    # 空配列だけを返すと LLM は「調べていない」と受け取り、勝手に推測を埋める。
    # 実測(2026-08-27)では時間帯を揃えると先行活動は全て比<=1 で、
    # 週次FBが出した「長い昼寝の直前にブラウジング逃避が先行する」は
    # 時間帯の交絡（ブラウジングも昼寝も午後に多い）だった。
    summary["negative_result"] = None if elevated else (
        f"{summary['tested']}件の「出来事×直前の活動」を検定したが、"
        "同じ時間帯のベースラインを超えて先行する活動は1件も無かった。"
        "つまり昼寝や就寝の直前に特別な行動パターンは存在しない。"
        "『〜の直前に〜していた』と語ってはいけない（時間帯の偏りを因果と誤読することになる）。"
    )
    return {"candidates": elevated, "summary": summary}


# ─────────────────────────────────────────────────────────────
# 3. 不在
# ─────────────────────────────────────────────────────────────
_ABSENCE_SQL = """
WITH days AS (
    SELECT CAST(d AS DATE) AS target_date
    FROM UNNEST(sequence(DATE '{start}', DATE '{end}', INTERVAL '1' DAY)) AS t(d)
)
SELECT CAST(d.target_date AS VARCHAR) AS target_date,
       f.target_date IS NULL OR f.total_minutes_asleep IS NULL OR f.total_minutes_asleep = 0 AS no_sleep,
       f.weight_kg IS NULL AS no_weight,
       a.target_date IS NULL OR a.calories_kcal IS NULL OR a.calories_kcal = 0 AS no_meal_log,
       a.lunch_calories IS NULL OR a.lunch_calories = 0 AS no_lunch,
       p.target_date IS NULL AS no_phone,
       w.target_date IS NULL OR w.work_core_sec = 0 AS no_work,
       r.target_date IS NULL OR r.signals_available < 3 AS no_recovery
FROM days d
LEFT JOIN iceberg.life_gold.mrt_fitness_daily_summary f ON d.target_date = f.target_date
LEFT JOIN iceberg.life_gold.mrt_asken a ON d.target_date = a.target_date
LEFT JOIN iceberg.life_gold.mrt_ai_phone_daily p ON d.target_date = p.target_date
LEFT JOIN iceberg.life_gold.mrt_aw_daily_work_summary w ON d.target_date = w.target_date
LEFT JOIN iceberg.life_silver.fitbit_recovery r ON d.target_date = r.target_date
ORDER BY d.target_date
"""


@task(name="Discover absences", retries=1, retry_delay_seconds=30)
def discover_absences(start: str, end: str) -> dict:
    """「何が起きなかったか」を出す。

    ここが一番見落とされる。実測で直近30日に食事記録なし4日 / 睡眠データなし7日 /
    体重なし13日 / 作業ゼロ11日があった。

    ★重要★ 欠測には2種類あり、区別せずに扱うと解釈を誤る:
      ・計測の欠測（時計を外した等）→ 「良い」とも「悪い」とも読めない
      ・行動の不在（昼食を抜いた等）→ それ自体が事実
    どちらかを機械的に判定できないので、両方を「不在」として並べ、
    ラベルに区別を明記して LLM 側に判断させる。
    """
    df = TRINO.execute_query(_ABSENCE_SQL.format(start=start, end=end))
    rows = it._records(df)
    total = len(rows)
    if total == 0:
        return {"candidates": [], "summary": {"days": 0}}

    flags = {
        "no_sleep": ("睡眠データなし", "計測欠測の可能性が高い（徹夜も含む）"),
        "no_weight": ("体重の記録なし", "行動の不在（体重計に乗っていない）"),
        "no_meal_log": ("食事記録なし", "記録の不在。実際に食べていないとは限らない"),
        "no_lunch": ("昼食の記録なし", "行動の不在の可能性が高い（他の食事は記録されている日が多い）"),
        "no_phone": ("スマホデータなし", "計測欠測（同期停止・クラスタ障害）"),
        "no_work": ("作業がゼロ", "行動の不在。休日なら正常"),
        "no_recovery": ("回復指標が3つ未満", "計測欠測。悪い夜に落ちるMNARなので改善と読まない"),
    }
    candidates = []
    for key, (label, note) in flags.items():
        missing_days = [r["target_date"] for r in rows if r.get(key)]
        if not missing_days:
            continue
        candidates.append({
            "kind": "absence",
            "what": label,
            "days_missing": len(missing_days),
            "days_total": total,
            "pct": round(100.0 * len(missing_days) / total, 1),
            "recent_dates": missing_days[-8:],
            "interpretation_note": note,
        })
    candidates.sort(key=lambda x: -x["days_missing"])
    return {
        "candidates": candidates,
        "summary": {
            "days": total,
            "note": (
                "不在は検定にかけていない（母集団が『起きなかったこと』なので p 値の意味が薄い）。"
                "件数と割合をそのまま示すので、計測の欠測か行動の不在かは "
                "interpretation_note を見て判断すること。"
            ),
        },
    }


# ─────────────────────────────────────────────────────────────
@task(name="Run structural discovery")
def run_discovery(end_date: str, lookback_days: int = LOOKBACK_DAYS) -> dict:
    """3種の構造的探索をまとめて回す。週次FBのコンテキストに入れる。"""
    import datetime

    end = datetime.date.fromisoformat(end_date)
    start = (end - datetime.timedelta(days=lookback_days)).isoformat()

    overlaps = discover_overlaps(start)
    transitions = discover_transitions(start)
    absences = discover_absences(start, end_date)

    print(
        f"🔬 発見エンジン: 重なり {overlaps['summary']['survived']}/{overlaps['summary']['tested']}件 / "
        f"遷移 {transitions['summary']['survived']}/{transitions['summary']['tested']}件 / "
        f"不在 {len(absences['candidates'])}件"
    )
    return {
        "window": f"{start} 〜 {end_date}",
        "overlaps": overlaps,
        "transitions": transitions,
        "absences": absences,
        "note": (
            "これは機械的な総当たりの結果で、**候補**でしかない。"
            "重なりと遷移は BH-FDR を通過したものだけが載っている（LLM は再検定しないこと）。"
            "因果の向きはここでは判定していないので、どちらが原因かは別途考えること。"
        ),
    }
