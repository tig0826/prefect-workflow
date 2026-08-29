"""AI フィードバックの「状態」を読み書きするレイヤ。

日次 flow と週次 flow の両方から使う。テーブル定義は sql/create_state_tables.sql。

設計の背景:
  従来の日次FBは状態を持たず、毎日ゼロから同じ入力を見て同じ結論に到達していた
  （直近30日70件の実測で「深夜」言及 81%、「概日リズム」49%）。
  プロンプトに「繰り返すな」と書いても原理的に効かない。生活習慣は日次では
  変化しないので、正しく分析するほど結論は同じになる。

  ここでの解決は「FB ができることを構造的に制限する」こと:
    ・新しい issue を立てる
    ・既存 issue の metric がどう動いたか報告する
  のどちらかだけにする。metric が動かないまま言及回数が閾値を超えたら、
  仮説の棄却を機械的に強制する（stale_issues → abandon_issue）。
"""

import datetime
import json
import re
import uuid
from zoneinfo import ZoneInfo

from common.trino_api import TrinoAPI

JST = ZoneInfo("Asia/Tokyo")
TRINO = TrinoAPI(host="trino.mynet", port=80, user="tig", catalog="iceberg")

# 同じ issue をこの回数言及して metric が動かなければ「仮説が外れた」と判定する。
# 4 は「1週間弱ずっと同じことを言い続けたら諦める」という意味。
MENTION_LIMIT_BEFORE_ABANDON = 4

# metric が「動いた」と見なす最小変化率。測定ノイズを改善と誤読しないため。
MIN_MEANINGFUL_CHANGE_PCT = 10.0


def _q(v) -> str:
    """SQL リテラルへ。None は NULL、文字列はエスケープ。"""
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


def _records(df) -> list[dict]:
    """DataFrame を dict のリストにする。**JSON にできない値を入口で正規化する。**

    ・NaN / NaT → None
      pandas は SQL の NULL を NaN(float) にするため、そのまま json.dumps すると
      `NaN` という不正な JSON を吐き、LLM 側のパースやプロンプト埋め込みで壊れる。
      さらに `x or default` のようなコードで NaN が truthy 判定されて
      「NULL のはずが値がある」ように振る舞う。
    ・Decimal → int / float
      Trino の round() や decimal 型は Decimal で返り、json.dumps が
      TypeError になる（default=str で逃げると数値が文字列になって比較が壊れる）。
    """
    import decimal

    import pandas as pd

    def norm(v):
        if isinstance(v, decimal.Decimal):
            # 整数で表せるものは int にして、プロンプト上の見た目を素直にする
            return int(v) if v == v.to_integral_value() else float(v)
        try:
            return None if pd.isna(v) else v
        except (TypeError, ValueError):
            return v

    if df.empty:
        return []
    return [{k: norm(v) for k, v in row.items()} for row in df.to_dict("records")]


# ─────────────────────────────────────────────────────────────
# 読み取り
# ─────────────────────────────────────────────────────────────
def load_active_issues() -> list[dict]:
    """open / testing の issue を返す（古い順）。"""
    df = TRINO.execute_query("""
        SELECT issue_id, CAST(opened_date AS VARCHAR) AS opened_date, title, hypothesis,
               discovered_by, evidence, metric_sql, metric_name, metric_unit,
               baseline_value, target_value, target_direction, status,
               mention_count, CAST(last_mentioned_date AS VARCHAR) AS last_mentioned_date, notes
        FROM iceberg.life_gold.ai_feedback_issues
        WHERE status IN ('open', 'testing')
        ORDER BY opened_date
    """)
    return _records(df)


def load_interventions(limit_days: int = 90) -> list[dict]:
    """直近の介入履歴。週次の前後比較の基準日になる。"""
    df = TRINO.execute_query(f"""
        SELECT intervention_id, issue_id,
               CAST(started_at AS VARCHAR) AS started_at,
               CAST(ended_at AS VARCHAR) AS ended_at,
               kind, description, config_ref, created_by
        FROM iceberg.life_gold.ai_interventions
        WHERE started_at >= date_add('day', -{limit_days}, current_timestamp)
        ORDER BY started_at DESC
    """)
    return _records(df)


def metric_history(issue_id: str, days: int = 28) -> list[dict]:
    """metric の時系列。「効いたか」を言うための土台。"""
    df = TRINO.execute_query(f"""
        SELECT CAST(eval_date AS VARCHAR) AS eval_date, metric_value, adherence_pct, eval_error
        FROM iceberg.life_gold.ai_metric_history
        WHERE issue_id = {_q(issue_id)}
          AND eval_date >= date_add('day', -{days}, current_date)
        ORDER BY eval_date
    """)
    return _records(df)


# ─────────────────────────────────────────────────────────────
# metric 評価
# ─────────────────────────────────────────────────────────────
def evaluate_metric(issue: dict, eval_date: str) -> tuple[float | None, str | None]:
    """issue の metric_sql を実行して (値, エラー) を返す。

    metric_sql の契約:
      ・1行1列の数値を返す
      ・日付は {eval_date} プレースホルダで受ける
        （current_date を使わない。Trino のセッションTZで日付がずれるため）

    値が取れない場合は必ずエラー文字列を返す。**欠損を 0 や「改善」として
    扱ってはいけない**ので、呼び出し側は None を「未評価」として扱う。
    """
    sql = issue.get("metric_sql")
    if not sql:
        return None, "metric_sql が未設定"
    try:
        rendered = sql.replace("{eval_date}", eval_date)
    except Exception as e:  # noqa: BLE001
        return None, f"プレースホルダ置換に失敗: {e}"

    try:
        df = TRINO.execute_query(rendered)
    except Exception as e:  # noqa: BLE001
        return None, f"SQL 実行エラー: {str(e)[:300]}"

    if df.empty or df.shape[1] < 1:
        return None, "結果が空"
    raw = df.iloc[0, 0]
    if raw is None:
        return None, "値が NULL（該当期間のデータ欠損の可能性）"
    try:
        return float(raw), None
    except (TypeError, ValueError):
        return None, f"数値に変換できない値: {raw!r}"


def record_metric(issue_id: str, eval_date: str, value: float | None,
                  adherence_pct: float | None = None, error: str | None = None) -> None:
    TRINO.execute_action(f"""
        INSERT INTO iceberg.life_gold.ai_metric_history
          (issue_id, eval_date, metric_value, adherence_pct, eval_error, evaluated_at)
        VALUES ({_q(issue_id)}, DATE {_q(eval_date)}, {_q(value)}, {_q(adherence_pct)},
                {_q(error)}, CURRENT_TIMESTAMP)
    """)


def evaluate_all_issues(eval_date: str) -> list[dict]:
    """全 active issue の metric を評価して履歴に積み、進捗つきで返す。

    返す各要素は issue の内容 + 以下:
      current_value / eval_error / change_from_baseline_pct / progress_pct /
      is_moving / history
    """
    results = []
    for issue in load_active_issues():
        value, error = evaluate_metric(issue, eval_date)
        record_metric(issue["issue_id"], eval_date, value, None, error)

        baseline = issue.get("baseline_value")
        target = issue.get("target_value")
        direction = issue.get("target_direction")

        change_pct = None
        progress_pct = None
        if value is not None and baseline not in (None, 0):
            change_pct = round((value - baseline) / abs(baseline) * 100, 1)
        if value is not None and baseline is not None and target is not None and target != baseline:
            # baseline → target を 0→100% として、今どこにいるか
            progress_pct = round((value - baseline) / (target - baseline) * 100, 1)

        # 「動いた」判定は方向つき。目標と逆に動いた場合は is_moving=False。
        is_moving = False
        if change_pct is not None and abs(change_pct) >= MIN_MEANINGFUL_CHANGE_PCT:
            if direction == "decrease":
                is_moving = change_pct < 0
            elif direction == "increase":
                is_moving = change_pct > 0

        results.append({
            **issue,
            "current_value": value,
            "eval_error": error,
            "change_from_baseline_pct": change_pct,
            "progress_pct": progress_pct,
            "is_moving": is_moving,
            "history": metric_history(issue["issue_id"], days=28),
        })
    return results


# ─────────────────────────────────────────────────────────────
# 書き込み
# ─────────────────────────────────────────────────────────────
def create_issue(title: str, hypothesis: str, discovered_by: str, evidence: dict,
                 metric_sql: str, metric_name: str, metric_unit: str,
                 baseline_value: float, target_value: float, target_direction: str,
                 opened_date: str | None = None, status: str = "open",
                 notes: str | None = None) -> str:
    """新しい issue を立てる。metric_sql が空なら例外（処方の必須条件）。

    ここで metric を強制することが「浅いアドバイス」を機械的に殺す仕掛け。
    「十分な睡眠を」「無理は禁物」は metric_sql を書けないので通らない。
    """
    if not metric_sql or not metric_sql.strip():
        raise ValueError(
            "metric_sql は必須。既存テーブルのカラムと閾値で自動評価できない処方は "
            "issue にできない（『十分な睡眠を』のような検証不能な助言を排除するため）"
        )
    if target_direction not in ("decrease", "increase"):
        raise ValueError(f"target_direction は decrease / increase のみ: {target_direction!r}")

    # mrt_ai_activity_hourly を source 横断で合算する metric を拒否する。
    # 同じ行動が window / media / web に重複して立つため二重計上になる。
    # 実際に週次FBの LLM が `source IN ('window','web')` で書き、0-4時の画面時間を
    # 258.4分/日 と報告した（正しくは 222.9分/日）。プロンプトの警告だけでは防げなかった。
    low = metric_sql.lower()
    if "mrt_ai_activity_hourly" in low and re.search(r"\b(sum|avg)\s*\(\s*(seconds|minutes)\b", low):
        if not re.search(r"source\s*=\s*'", low):
            raise ValueError(
                "mrt_ai_activity_hourly の seconds/minutes を集計するときは "
                "source = '...' で1つに絞ること（source をまたぐと同じ行動が二重に数えられる）。"
                "画面時間の総量が欲しい場合は mrt_ai_screen_hourly.screen_minutes を使う"
                "（分単位で重複排除済み）。"
            )

    issue_id = f"ISS-{uuid.uuid4().hex[:8].upper()}"
    opened = opened_date or datetime.datetime.now(JST).strftime("%Y-%m-%d")
    TRINO.execute_action(f"""
        INSERT INTO iceberg.life_gold.ai_feedback_issues
          (issue_id, opened_date, title, hypothesis, discovered_by, evidence,
           metric_sql, metric_name, metric_unit, baseline_value, target_value,
           target_direction, status, mention_count, last_mentioned_date,
           resolved_date, notes, created_at, updated_at)
        VALUES ({_q(issue_id)}, DATE {_q(opened)}, {_q(title)}, {_q(hypothesis)},
                {_q(discovered_by)}, {_q(json.dumps(evidence, ensure_ascii=False))},
                {_q(metric_sql)}, {_q(metric_name)}, {_q(metric_unit)},
                {_q(baseline_value)}, {_q(target_value)}, {_q(target_direction)},
                {_q(status)}, 0, NULL, NULL, {_q(notes)},
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
    """)
    return issue_id


def bump_mention(issue_id: str, mentioned_date: str) -> None:
    """言及回数を1つ増やす。同じ日に二重に増やさない。"""
    TRINO.execute_action(f"""
        UPDATE iceberg.life_gold.ai_feedback_issues
        SET mention_count = mention_count + 1,
            last_mentioned_date = DATE {_q(mentioned_date)},
            updated_at = CURRENT_TIMESTAMP
        WHERE issue_id = {_q(issue_id)}
          AND (last_mentioned_date IS NULL OR last_mentioned_date < DATE {_q(mentioned_date)})
    """)


def set_status(issue_id: str, status: str, note: str | None = None) -> None:
    resolved = "CURRENT_DATE" if status in ("resolved", "abandoned", "rejected") else "NULL"
    note_sql = (
        f"notes = CONCAT(COALESCE(notes, ''), {_q(chr(10) + str(note))})" if note else "notes = notes"
    )
    TRINO.execute_action(f"""
        UPDATE iceberg.life_gold.ai_feedback_issues
        SET status = {_q(status)},
            resolved_date = {resolved},
            {note_sql},
            updated_at = CURRENT_TIMESTAMP
        WHERE issue_id = {_q(issue_id)}
    """)


def record_intervention(description: str, kind: str, issue_id: str | None = None,
                        config_ref: str | None = None, created_by: str = "weekly_llm",
                        started_at: str | None = None, notes: str | None = None) -> str:
    intervention_id = f"INT-{uuid.uuid4().hex[:8].upper()}"
    start = started_at or datetime.datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")
    TRINO.execute_action(f"""
        INSERT INTO iceberg.life_gold.ai_interventions
          (intervention_id, issue_id, started_at, ended_at, kind, description,
           config_ref, created_by, notes, created_at)
        VALUES ({_q(intervention_id)}, {_q(issue_id)}, TIMESTAMP {_q(start)}, NULL,
                {_q(kind)}, {_q(description)}, {_q(config_ref)}, {_q(created_by)},
                {_q(notes)}, CURRENT_TIMESTAMP)
    """)
    return intervention_id


# ─────────────────────────────────────────────────────────────
# 仮説の棄却
# ─────────────────────────────────────────────────────────────
def enforce_abandonment(evaluated: list[dict]) -> list[dict]:
    """言及回数が上限を超え、かつ metric が動いていない issue を abandoned にする。

    これが「毎日同じことを言い続ける」を構造的に不可能にする本体。
    棄却された issue は次回から active に出てこないので、FB は別の仮説を
    立てざるを得なくなる。
    """
    abandoned = []
    for e in evaluated:
        if e.get("status") != "open":
            continue
        if (e.get("mention_count") or 0) < MENTION_LIMIT_BEFORE_ABANDON:
            continue
        # 評価できていない（データ欠損）ものは棄却しない。欠損は無効果ではない。
        if e.get("current_value") is None:
            continue
        if e.get("is_moving"):
            continue
        reason = (
            f"[auto] {e['mention_count']}回言及したが metric「{e.get('metric_name')}」が "
            f"baseline {e.get('baseline_value')} → {e.get('current_value')} "
            f"({e.get('change_from_baseline_pct')}%) で有意に動かなかったため仮説を棄却。"
        )
        set_status(e["issue_id"], "abandoned", reason)
        e["status"] = "abandoned"
        e["abandon_reason"] = reason
        abandoned.append(e)
        print(f"🪦 abandoned {e['issue_id']}: {e.get('title')}")
    return abandoned


# ─────────────────────────────────────────────────────────────
# 計装提案（新しく取るべきデータ）
# ─────────────────────────────────────────────────────────────
def load_instrumentation_proposals() -> list[dict]:
    """過去の計装提案。クリティックに渡して重複提案を防ぐ。"""
    df = TRINO.execute_query("""
        SELECT proposal_id, question, missing_data, how_to_collect_passively,
               would_enable, status, proposal_count,
               CAST(first_proposed_date AS VARCHAR) AS first_proposed_date,
               CAST(last_proposed_date AS VARCHAR) AS last_proposed_date, notes
        FROM iceberg.life_gold.ai_instrumentation_proposals
        ORDER BY proposal_count DESC, first_proposed_date
    """)
    return _records(df)


def record_instrumentation_proposals(proposals: list[dict], proposed_date: str) -> dict:
    """クリティックの提案を保存する。

    既存提案と同じものは `existing_proposal_id` を LLM に指定させ、
    ここでは件数と最終提案日を更新するだけにする。
    文言は毎週変わるのでハッシュによる重複判定は効かず、
    「同じ問いか」の判断は LLM にやらせるのが妥当（文の類似はLLMの得意分野）。

    返り値: {"new": [...], "bumped": [...], "skipped": [...]}
    """
    import uuid as _uuid

    new_ids, bumped_ids, skipped = [], [], []
    for p in proposals or []:
        question = (p.get("question") or "").strip()
        if not question:
            continue
        # 「取れたら何が判定できるようになるか」が書けない提案は採用しない
        if not (p.get("would_enable") or "").strip():
            skipped.append({"question": question[:80], "reason": "would_enable が空"})
            continue
        # 受動的に取れないものは記録するが status で区別する
        passive = (p.get("how_to_collect_passively") or "").strip()
        status = "proposed" if passive and "受動的手段なし" not in passive else "rejected"
        note = None if status == "proposed" else "[auto] 受動的に取得できないため却下（手入力はMNARで死ぬ）"

        existing_id = (p.get("existing_proposal_id") or "").strip()
        if existing_id:
            TRINO.execute_action(f"""
                UPDATE iceberg.life_gold.ai_instrumentation_proposals
                SET proposal_count = proposal_count + 1,
                    last_proposed_date = DATE {_q(proposed_date)},
                    updated_at = CURRENT_TIMESTAMP
                WHERE proposal_id = {_q(existing_id)}
                  AND (last_proposed_date IS NULL OR last_proposed_date < DATE {_q(proposed_date)})
            """)
            bumped_ids.append(existing_id)
            continue

        pid = f"INS-{_uuid.uuid4().hex[:8].upper()}"
        TRINO.execute_action(f"""
            INSERT INTO iceberg.life_gold.ai_instrumentation_proposals
              (proposal_id, dedup_key, first_proposed_date, last_proposed_date, proposal_count,
               question, missing_data, how_to_collect_passively, would_enable,
               status, notes, created_at, updated_at)
            VALUES ({_q(pid)}, {_q(question[:120])}, DATE {_q(proposed_date)}, DATE {_q(proposed_date)}, 1,
                    {_q(question)}, {_q(p.get('missing_data'))}, {_q(passive)},
                    {_q(p.get('would_enable'))}, {_q(status)}, {_q(note)},
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """)
        new_ids.append(pid)

    return {"new": new_ids, "bumped": bumped_ids, "skipped": skipped}


def set_proposal_status(proposal_id: str, status: str, note: str | None = None) -> None:
    """提案の状態を変える。'already_satisfied' は配線漏れが原因の再提案に使う。"""
    note_sql = (
        f"notes = CONCAT(COALESCE(notes, ''), {_q(chr(10) + str(note))})" if note else "notes = notes"
    )
    TRINO.execute_action(f"""
        UPDATE iceberg.life_gold.ai_instrumentation_proposals
        SET status = {_q(status)}, {note_sql}, updated_at = CURRENT_TIMESTAMP
        WHERE proposal_id = {_q(proposal_id)}
    """)
