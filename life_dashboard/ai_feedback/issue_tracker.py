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

from ai_feedback import github_tickets as gt
from common.trino_api import TrinoAPI

JST = ZoneInfo("Asia/Tokyo")
TRINO = TrinoAPI(host="trino.mynet", port=80, user="tig", catalog="iceberg")

# 同じ issue をこの回数言及して metric が動かなければ「仮説が外れた」と判定する。
# 4 は「1週間弱ずっと同じことを言い続けたら諦める」という意味。
MENTION_LIMIT_BEFORE_ABANDON = 4

# 目標を何日連続で満たしたら仮説の検証を完了とみなすか。
# 1日でも満たせば卒業にすると、ノイズで上振れした日に誤って外れる。
GRADUATION_DAYS = 5

# verifying に上がった仮説を閉じるまでに、目標を何日連続で満たし続ける必要があるか。
# 卒業（5日）から少なくとも1週間強は様子を見る。一時的に下がっただけの効果で閉じないため。
VERIFY_DAYS_BEFORE_CLOSE = 14

# verifying の仮説が、直近この日数すべてで目標を外したら testing に戻す（後戻りの検出）。
REGRESSION_DAYS = 3

# 日次で指標を評価する仮説の status。実況するのは open/testing だけで、
# verifying は「閉じるか戻すか」を判定するためだけに評価を続ける。
EVALUATED_STATUSES = ("open", "testing", "verifying")
NARRATED_STATUSES = ("open", "testing")

# metric が「動いた」と見なす最小変化率。測定ノイズを改善と誤読しないため。
MIN_MEANINGFUL_CHANGE_PCT = 10.0

# 介入を打ってから、効果を判定してよくなるまでの最低日数。
# 昨日打った手を今日の数字で否定するのは不公平なので待つ。
MIN_INTERVENTION_DAYS = 7


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
    """検証中の仮説を GitHub から読む。

    ★2026-09-03: 出どころを Trino から GitHub に移した★
    課題管理は GitHub の Issue が正になった。本人が仮説と指標の定義を読めて
    直せる必要があるため（「今の所あなたが課題とか色々言ってるのが何もわからない」）。
    Trino に残すのは ai_metric_history（時系列）だけ。

    issue_id は GitHub の issue 番号（例 "13"）。旧 "ISS-xxx" 形式は使わない。

    ★2026-10-03: verifying も返す★
    日次で実況するのは open/testing だけ（呼び出し側で NARRATED_STATUSES で絞る）。
    verifying は「安定したので閉じる / 後戻りしたので戻す」の判定のために評価だけ続ける。
    """
    rows = []
    for h in gt.load_active_hypotheses(statuses=EVALUATED_STATUSES):
        rows.append(
            {
                "issue_id": str(h["number"]),
                "title": h["title"],
                "hypothesis": h["body"],
                "status": h["status"],
                "metric_name": h["metric_name"],
                "metric_unit": h["metric_unit"],
                "metric_sql": h["metric_sql"],
                "baseline_value": h["baseline_value"],
                "target_value": h["target_value"],
                "target_direction": h["target_direction"],
                "mention_count": h["mention_count"],
                "parent": h.get("parent"),
            }
        )
    return rows


def load_problems() -> list[dict]:
    """実在する課題（severity 付き）。**仮説とは別物。**

    日次FBはこれを見て「何が重いか」を判断する。
    以前はこの層が存在せず、仮説を課題として扱っていたため、
    仮説が反証されると課題まで閉じていた。
    """
    return gt.load_problems()


def load_unaddressed_problems() -> list[dict]:
    """仮説が1件も紐付いていない課題（life:problem）。severity/priority順。

    ★2026-09-12: 追加した理由★
    週次分析は `load_active_issues`（仮説層）しか見ておらず、仮説が一度も
    立っていない課題は週次LLMの視界に入っていなかった。実際に sev:S1/pri:P0
    クラスの課題が起票から数週間、仮説ゼロで放置される実害が出た。

    `gt.parent_map()` は「課題(open)→そのsub-issue全部」の対応表なので、
    その値（＝何かしら子issueが紐付いている課題番号の集合）に入っていない
    課題を「仮説ゼロ」とみなす。
    """
    problems = gt.load_problems()
    covered = set(gt.parent_map().values())
    return [
        {
            "number": p["number"],
            "title": p["title"],
            "severity": p["severity"],
            "priority": p["priority"],
            "body": (p["body"] or "")[:500],
        }
        for p in problems
        if p["number"] not in covered
    ]


def load_interventions(limit_days: int = 90) -> list[dict]:
    """直近の「打った手」（life:action）。週次の前後比較の基準日になる。

    ★2026-09-03: 出どころを Trino から GitHub に移した★
    打った手は life:action として GitHub にあり、
    「効いたかの確かめ方」や検証結果がコメントに書かれている。
    Trino の ai_interventions を併用すると出どころが2つになるので使わない。
    """
    rows = gt._call(f"/repos/{gt.REPO}/issues?labels=life:action&state=all&per_page=100")
    cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=limit_days)
    out = []
    for a in rows or []:
        if a.get("pull_request"):
            continue
        started = datetime.datetime.fromisoformat(a["created_at"].replace("Z", "+00:00"))
        if started < cutoff:
            continue
        out.append(
            {
                "intervention_id": f"#{a['number']}",
                "issue_id": None,
                "started_at": a["created_at"],
                "ended_at": a.get("closed_at"),
                "kind": gt._label_value(a, "kind:") or "",
                "description": a["title"].replace("[打った手] ", ""),
                "created_by": "github",
            }
        )
    return sorted(out, key=lambda x: x["started_at"], reverse=True)


def interventions_for_issue(issue_id: str) -> list[dict]:
    """その仮説に対して実際に打たれた手を GitHub から返す。

    ★これが無いと「未検証」と「反証」を区別できない★
    metric が動かない理由は3つある:
      1. 仮説が間違っている
      2. 仮説は正しいが、有効な介入を打っていない
      3. 介入は打ったが守られていない
    介入の有無を見ずに棄却すると、3つ全部を「1」と判定してしまう。
    実測で、旧実装が棄却した2件はどちらも介入0件だった。
    """
    now = datetime.datetime.now(datetime.timezone.utc)
    out = []
    for a in gt.load_actions_for(int(issue_id)):
        started = datetime.datetime.fromisoformat(a["created_at"].replace("Z", "+00:00"))
        out.append(
            {
                "intervention_id": f"#{a['number']}",
                "kind": a["kind"],
                "description": a["title"],
                "started_at": a["created_at"],
                "days_since_start": (now - started).days,
            }
        )
    return out


def metric_history(issue_id: str, days: int = 28) -> list[dict]:
    """metric の時系列（1日1行）。「効いたか」を言うための土台。

    ★2026-10-03: 日付で重複を除く★
    ai_metric_history には同じ日の行が複数入る（実測: 9/30 だけで4行）。
    日次の手動リラン、週次の evaluate_all_issues（week_end が日次の評価日と重なる）が
    それぞれ INSERT するため。重複のまま「連続○日達成」を数えると、
    2日分しか無いのに5日連続と誤判定する。値の取れた行を優先し、その中で最新を採る。
    """
    df = TRINO.execute_query(f"""
        SELECT CAST(eval_date AS VARCHAR) AS eval_date, metric_value, adherence_pct, eval_error
        FROM (
          SELECT eval_date, metric_value, adherence_pct, eval_error,
                 row_number() OVER (
                   PARTITION BY eval_date
                   ORDER BY CASE WHEN metric_value IS NULL THEN 1 ELSE 0 END, evaluated_at DESC
                 ) AS rn
          FROM iceberg.life_gold.ai_metric_history
          WHERE issue_id = {_q(issue_id)}
            AND eval_date >= date_add('day', -{days}, current_date)
        )
        WHERE rn = 1
        ORDER BY eval_date
    """)
    return _records(df)


def daily_values(history: list[dict] | None) -> list[tuple[str, float]]:
    """履歴を (日付, 値) の昇順リストにする。値の無い日は落とし、同じ日は最後の1件だけ残す。

    ★履歴のキーは metric_value★
    以前 enforce_graduation は `h["v"]` を読んでいたが、evaluate_all_issues が返す
    履歴のキーは `metric_value` で、`{"d","v"}` への変換は LLM に渡す ctx 側でしか
    していなかった。そのため卒業判定に届く履歴は常に空で、連続日数は毎回 0、
    卒業は入れた日から一度も起きていなかった（#7 は 9/9 以降ずっと目標以下なのに
    testing のまま毎朝実況されていた）。履歴の解釈はここに一本化する。
    """
    by_date: dict[str, float] = {}
    for h in history or []:
        v = h.get("metric_value")
        d = h.get("eval_date")
        if v is None or d is None:
            continue
        by_date[str(d)[:10]] = float(v)
    return sorted(by_date.items())


def _meets(v: float, target: float, direction: str) -> bool:
    return v <= target if direction == "decrease" else v >= target


def target_streak(e: dict) -> int:
    """履歴の末尾から、目標を連続で満たしている日数。目標・方向が無ければ 0。"""
    target = e.get("target_value")
    direction = e.get("target_direction")
    if target is None or direction not in ("decrease", "increase"):
        return 0
    streak = 0
    for _, v in reversed(daily_values(e.get("history"))):
        if not _meets(v, target, direction):
            break
        streak += 1
    return streak


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
                 notes: str | None = None, parent_number: int | None = None) -> str:
    """新しい仮説を GitHub に起票する。metric_sql が空なら例外。

    ここで metric を強制することが「浅いアドバイス」を機械的に殺す仕掛け。
    「十分な睡眠を」「無理は禁物」は metric_sql を書けないので通らない。

    ★2026-09-03: 起票先を GitHub に変えた★
    指標の定義は本文の <!-- metric --> と ```sql ブロックに入る。
    本人が読んで直せることが要件なので、Trino のカラムに埋めない。

    返すのは GitHub の issue 番号（文字列）。
    """
    if not metric_sql or not metric_sql.strip():
        raise ValueError(
            "metric_sql は必須。既存テーブルのカラムと閾値で自動評価できない処方は "
            "issue にできない（『十分な睡眠を』のような検証不能な助言を排除するため）"
        )
    if target_direction not in ("decrease", "increase"):
        raise ValueError(f"target_direction は decrease / increase のみ: {target_direction!r}")

    # mrt_ai_activity_hourly を source 横断で集計する metric を拒否する。
    # 同じ行動が window / media / web に重複して立つため二重計上になる。
    # 実際に週次FBの LLM が `source IN ('window','web')` で書き、0-4時の画面時間を
    # 258.4分/日 と報告した（正しくは 222.9分/日）。プロンプトの警告では防げなかった。
    low = metric_sql.lower()
    if "mrt_ai_activity_hourly" in low and re.search(r"\b(sum|avg)\s*\(\s*(seconds|minutes)\b", low):
        if not re.search(r"source\s*=\s*'", low):
            raise ValueError(
                "mrt_ai_activity_hourly の seconds/minutes を集計するときは "
                "source = '...' で1つに絞ること（source をまたぐと同じ行動が二重に数えられる）。"
                "画面時間の総量が欲しい場合は mrt_ai_screen_hourly.screen_minutes を使う"
                "（分単位で重複排除済み）。"
            )

    body = f"""## 仮説
{hypothesis}

## 支持するデータ
{json.dumps(evidence, ensure_ascii=False, indent=2)}

<!-- metric
name: {metric_name}
unit: {metric_unit}
baseline: {baseline_value}
target: {target_value}
direction: {target_direction}
mention_count: 0
-->

## 検証指標

| | 値 |
|---|---|
| 指標名 | {metric_name} |
| 単位 | {metric_unit} |
| baseline | {baseline_value} |
| 目標 | {target_value} |
| 方向 | {target_direction} |

```sql
{metric_sql.strip()}
```

<sub>この SQL が日次フローで実行され、結果が `ai_metric_history` に積まれます。
**測り方が違うと思ったら SQL を直してください。** `<!-- metric -->` の
baseline / target / direction も同時に直すこと（機械が読むのはそちら）。</sub>
"""
    if notes:
        body += f"\n## 補足\n{notes}\n"

    labels = ["life:hypothesis", f"status:{status}"]
    by = {
        "weekly_llm": "by:weekly-llm",
        "chat": "by:chat",
        "manual": "by:manual",
    }.get(discovered_by, "by:structural")
    labels.append(by)

    created = gt._call(
        f"/repos/{gt.REPO}/issues",
        "POST",
        {"title": title, "body": body, "labels": labels},
    )
    number = created["number"]

    if parent_number:
        try:
            gt._call(
                f"/repos/{gt.REPO}/issues/{parent_number}/sub_issues",
                "POST",
                {"sub_issue_id": created["id"]},
            )
        except Exception as e:  # noqa: BLE001
            print(f"⚠️ 親 #{parent_number} への紐付けに失敗: {e}")

    del opened_date  # GitHub の created_at が起票日
    return str(number)


def bump_mention(issue_id: str, mentioned_date: str) -> None:
    """言及回数を1つ増やす（GitHub の本文の metric ブロックに持つ）。

    ★カウンタだけ Trino に残さない★
    課題管理を GitHub に一本化した以上、カウンタを別の場所に置くと
    「同じ情報の出どころが2つ」に戻る。実際にその構図で
    「間違った方を読む」事故を2回起こしている。
    """
    del mentioned_date  # GitHub 側は最終言及日を timeline が持つので不要
    gt.bump_mention(int(issue_id))


def set_status(issue_id: str, status: str, note: str | None = None) -> None:
    """仮説の status ラベルを張り替え、理由をコメントで残す。

    ★親の課題は閉じない★
    仮説が反証されても、課題（life:problem）は実在するので生き続ける。
    以前は1行に事実と推測が同居していて、推測の反証で事実まで閉じていた。
    """
    gt.set_status_label(int(issue_id), status, note)


def record_intervention(description: str, kind: str, issue_id: str | None = None,
                        config_ref: str | None = None, created_by: str = "weekly_llm",
                        started_at: str | None = None, notes: str | None = None) -> str:
    """打った手を GitHub に life:action として起票する。

    ★2026-09-03: 書き込み先を Trino から GitHub に移した★
    打った手は仮説の子チケットになる。
    親（issue_id = 仮説の issue 番号）を必ず指定すること。
    親のない介入は「どの仮説に対して打ったのか不明」という意味で、
    効果測定のときに交絡要因として扱われる。

    ★started_at は「実際に効き始めた時刻」を書く★
    commit 時刻を使わない。実例: AGH v3 は 8/27 10:45 開始だが
    commit は 8/29 18:21 で2日ずれる。commit 日で期間を切ると前後比較がずれる。
    """
    body = f"""## 打った手
{description}

## 種別
`{kind}`

| | 値 |
|---|---|
| 開始 | {started_at or datetime.datetime.now(JST).strftime('%Y-%m-%d %H:%M')} |
| 終了 | 継続中 |
"""
    if config_ref:
        body += f"\n**設定の由来**: `{config_ref}`\n"
    if notes:
        body += f"\n## 補足\n{notes}\n"
    if issue_id:
        body += f"\n対象の仮説: #{issue_id}\n"
    else:
        body += (
            "\n> **対象の仮説が未設定です。** どの仮説に対して打った手なのかが不明で、"
            "効果測定のときに交絡要因として扱われます。\n"
        )

    created = gt._call(
        f"/repos/{gt.REPO}/issues",
        "POST",
        {
            "title": f"[打った手] {description[:70]}",
            "body": body,
            "labels": ["life:action", f"kind:{kind}"],
        },
    )
    if issue_id:
        try:
            gt._call(
                f"/repos/{gt.REPO}/issues/{issue_id}/sub_issues",
                "POST",
                {"sub_issue_id": created["id"]},
            )
        except Exception as e:  # noqa: BLE001
            print(f"⚠️ 仮説 #{issue_id} への紐付けに失敗: {e}")
    return f"#{created['number']}"


def enforce_abandonment(evaluated: list[dict]) -> list[dict]:
    """言及回数が上限を超え、かつ metric が動いていない issue を仕分ける。

    これが「毎日同じことを言い続ける」を構造的に不可能にする本体。

    ★2026-09-02 の修正: 「未検証」を「反証」として扱っていた★
    旧実装は「言及4回 + metric が動かない」だけで abandoned にしていた。
    しかし **mention_count は言及回数であって検証回数ではない。**
    実測で、棄却された2件（ISS-891764E7 / ISS-001）はどちらも
    **介入が一度も打たれていなかった**（ai_interventions に0件）。
    誰も動かそうとしていない指標が動かなかったことを理由に、
    仮説が「外れた」と記録されていた。ISS-001 は自分の仮説文に
    「介入すべきは深夜側」と書いたまま、深夜側に何も打たずに死んだ。

    放置すると「実際には検証していないのに検証済みとして残る」誤った知識が溜まる。
    そこで判定を2つに分ける:

      介入あり + 十分な期間経過 + 動かない → abandoned（本当の反証）
      介入なし                             → untested（未検証。試す価値は残っている）

    untested は active から外れるので繰り返しは止まるが、abandoned とは意味が違う。
    後から介入が紐づけば revive_untested() で testing に戻る。

    ★遵守率（adherence）について★
    ai_metric_history.adherence_pct は設計されているが 127行すべて NULL で、
    配線されていない。介入ごとに測り方が違うため汎用の計算ができない
    （dns_block なら「窓内に通過したクエリ数」で測れるが、AGH のクエリログが
    まだ Iceberg に入っていない）。よって現時点では「介入が存在し、
    MIN_INTERVENTION_DAYS 以上経過している」までを条件とする。
    遵守率が取れるようになったら、ここに条件を足す。

    ★未解決: testing の仮説は棄却されない（2026-10-03 時点で意図的に据え置き）★
    `status != "open"` で弾いているので、介入を打った（testing の）仮説は
    指標が動かなくても棄却されない。上の表の「介入あり + 期間経過 + 動かない」は
    本来 testing の仮説のことなので条件としてはずれている。
    ただし今の「介入」には打ち手でないものが混ざっている（#13 の介入 #3 は
    DQX のインストール＝原因そのもの）。#8 は指標＝打ち手の対象（画面時間）なので、
    打ち手が効かなかっただけで仮説が外れたわけではない。この状態で testing まで
    自動棄却すると誤った「反証済み」が増えるので、介入の質を判定できるまで広げない。
    """
    result = []
    for e in evaluated:
        if e.get("status") != "open":
            continue
        if (e.get("mention_count") or 0) < MENTION_LIMIT_BEFORE_ABANDON:
            continue
        # 評価できていない（データ欠損）ものは仕分けない。欠損は無効果ではない。
        if e.get("current_value") is None:
            continue
        if e.get("is_moving"):
            continue

        acted = [
            v
            for v in interventions_for_issue(e["issue_id"])
            if (v.get("days_since_start") or 0) >= MIN_INTERVENTION_DAYS
        ]

        if not acted:
            all_iv = interventions_for_issue(e["issue_id"])
            if all_iv:
                # 介入はあるが日が浅い。まだ判定しない。
                print(f"⏳ {e['issue_id']}: 介入から{max(v.get('days_since_start') or 0 for v in all_iv)}日。判定を保留")
                continue
            reason = (
                f"[auto] {e['mention_count']}回言及したが、この課題に対する介入が"
                f"一度も打たれていない（ai_interventions に0件）。"
                f"metric「{e.get('metric_name')}」が動かないのは当然なので、"
                f"仮説の反証ではなく**未検証**として扱う。"
                f"試す価値は残っているが、繰り返しを止めるため active から外す。"
                f"介入を打って紐付ければ revive_untested() で復帰する。"
            )
            set_status(e["issue_id"], "untested", reason)
            e["status"] = "untested"
            e["untested_reason"] = reason
            print(f"🧪 untested {e['issue_id']}: {e.get('title')}（介入0件）")
            result.append(e)
            continue

        applied = "; ".join(
            f"{v['intervention_id']}({v['kind']}, {str(v['started_at'])[:10]}〜)" for v in acted
        )
        reason = (
            f"[auto] {e['mention_count']}回言及し、介入も打った上で "
            f"metric「{e.get('metric_name')}」が "
            f"baseline {e.get('baseline_value')} → {e.get('current_value')} "
            f"({e.get('change_from_baseline_pct')}%) で有意に動かなかったため仮説を棄却。"
            f"打った介入: {applied}"
        )
        set_status(e["issue_id"], "abandoned", reason)
        e["status"] = "abandoned"
        e["abandon_reason"] = reason
        print(f"🪦 abandoned {e['issue_id']}: {e.get('title')}")
        result.append(e)
    return result


def enforce_graduation(evaluated: list[dict]) -> list[dict]:
    """目標を達成し続けている仮説を verifying に上げ、日次の実況から外す。

    ★これが無いと成功に出口が無い★
    enforce_abandonment は `is_moving` を素通りさせるので、効いている仮説は
    永久に open のまま毎朝実況され続ける。実測（2026-09-11）では #7
    「漫画アプリへの勤務中の逃避」が18日中13日 FB に登場していた。
    達成すればするほど死ななくなる構造になっていた。

    ★閉じずに verifying に上げる理由★
    仮説の metric を達成しても、それは**打った手が効いた**ことしか示さない。
    #7 の親は #21「娯楽への逃避が、介入を重ねても総量として減らない」であり、
    漫画が減っても逃避の総量が減っていなければ課題は解決していない
    （逃避先が移っただけの可能性がある）。
    したがって達成時に問いを切り替える:
        「漫画は減ったか」→「親の課題は動いたか」
    親が動けば solved、動かなければ仮説自体が誤りなので新しい仮説へ。
    親の課題の判断は親の issue 側で続ける。

    ★2026-10-03: 卒業後の出口を作った★
    以前は「その判断は本人と weekly に委ねる」と書かれていたが weekly 側は未実装で、
    verifying は行き止まりだった。今は enforce_verification が続きを引き受ける
    （VERIFY_DAYS_BEFORE_CLOSE 日連続で安定 → close / 後戻り → testing）。

    ★2026-10-03: 履歴の読み方のバグを直した★ daily_values の説明を参照。
    """
    graduated = []
    for e in evaluated:
        if e.get("status") not in NARRATED_STATUSES:
            continue
        # 履歴の末尾から連続で目標を満たしている日数を数える。
        # 単日の達成で卒業させると、ノイズで上振れした日に誤って外れる。
        streak = target_streak(e)
        if streak < GRADUATION_DAYS:
            continue
        target = e.get("target_value")

        note = (
            f"[auto] metric「{e.get('metric_name')}」が目標 {target} を "
            f"{streak}日連続で満たしたため、この仮説の検証は完了とみなす。"
            f"baseline {e.get('baseline_value')} → 現在 {e.get('current_value')}。\n\n"
            f"**打った手が効いたことは示されたが、親の課題が解決したとは限らない。**"
            f"以後の問いは「この指標が目標に届いたか」ではなく"
            f"「親の課題が動いたか」に切り替える。"
            f"親が動いていなければ、効果が別の行動に移っただけの可能性がある。\n\n"
            f"この仮説は日次の実況から外れる。指標の評価は続け、"
            f"{VERIFY_DAYS_BEFORE_CLOSE}日連続で目標を満たし続けたら自動で close、"
            f"直近{REGRESSION_DAYS}日すべてで目標を外したら testing に戻す。"
        )
        set_status(e["issue_id"], "verifying", note)
        e["status"] = "verifying"
        e["graduated_now"] = True  # 同じ実行で enforce_verification に閉じさせない
        e["graduation_note"] = note
        e["graduation_streak"] = streak
        print(f"🎓 verifying {e['issue_id']}: {e.get('title')}（{streak}日連続で目標達成）")
        graduated.append(e)
    return graduated


def enforce_verification(evaluated: list[dict]) -> tuple[list[dict], list[dict]]:
    """verifying の仮説を「閉じる / testing に戻す / 待つ」に仕分ける。

    ★これが無いと verifying が行き止まりになる★
    卒業（enforce_graduation）は実況から外すだけで、その先の出口が無かった。
    閉じる判断を人に委ねたまま誰も見に来ないので、成功した仮説ほど open のまま溜まる。

      目標を VERIFY_DAYS_BEFORE_CLOSE 日連続で満たしている → close（completed）
      直近 REGRESSION_DAYS 日すべてで目標を外した          → testing に戻して実況を再開
      それ以外                                           → 待つ

    閉じても親の課題（life:problem）は閉じない。仮説の検証が終わっただけで、
    課題が解決したかは課題の側で判断する。

    返り値: (閉じたもの, testing に戻したもの)
    """
    closed, regressed = [], []
    for e in evaluated:
        if e.get("status") != "verifying" or e.get("graduated_now"):
            continue
        target = e.get("target_value")
        direction = e.get("target_direction")
        if target is None or direction not in ("decrease", "increase"):
            continue
        vals = daily_values(e.get("history"))
        if not vals:
            continue

        streak = target_streak(e)
        if streak >= VERIFY_DAYS_BEFORE_CLOSE:
            note = (
                f"[auto] verifying に上がった後も、metric「{e.get('metric_name')}」が "
                f"{streak}日連続で目標 {target} を満たし続けたため、この仮説の検証を完了として close する。"
                f"baseline {e.get('baseline_value')} → 現在 {e.get('current_value')}。\n\n"
                f"**親の課題は閉じない。** 打った手が効いたことは確かめられたが、"
                f"課題そのものが解決したかは課題の側で判断する。"
                f"後で同じ問題がぶり返したら、この issue を reopen して status を testing に戻すこと。"
            )
            gt.close_issue(int(e["issue_id"]), "completed", note)
            e["status"] = "closed"
            e["close_kind"] = "verified"
            print(f"✅ closed(verified) {e['issue_id']}: {e.get('title')}（{streak}日連続）")
            closed.append(e)
            continue

        recent = vals[-REGRESSION_DAYS:]
        if len(recent) == REGRESSION_DAYS and not any(_meets(v, target, direction) for _, v in recent):
            note = (
                f"[auto] verifying の後、metric「{e.get('metric_name')}」が直近{REGRESSION_DAYS}日すべてで"
                f"目標 {target} を外した（{', '.join(f'{d}: {v}' for d, v in recent)}）。"
                f"効果が後戻りした可能性があるので testing に戻し、日次の実況を再開する。"
            )
            set_status(e["issue_id"], "testing", note)
            e["status"] = "testing"
            print(f"↩️ regressed {e['issue_id']}: {e.get('title')}")
            regressed.append(e)
    return closed, regressed


def process_close_requests() -> list[dict]:
    """本人が「閉じてよい」と言った issue（司書が req:close を付けたもの）を閉じる。

    ★なぜ司書に close させないか★
    ダッシュボード側は close を実装しない規約（github-tickets.ts の冒頭）。
    書き手が増えると状態を争って台帳が腐るので、close は日次フローに一本化する。
    司書は「本人がそう言った」という事実を `req:close` とコメントで残すだけ。

    ★以前は本人の判断が通らなかった★
    #7 には本人の「クローズでいい」がチャット経由で3回コメントされていたが、
    閉じる経路がどこにも無く open のままだった。

    種類（課題・仮説・打った手）は問わない。本人の判断を優先する。
    """
    out = []
    for i in gt.load_close_requests():
        gt.close_issue(
            i["number"],
            "completed",
            "[auto] 本人が会話の中で「閉じてよい」と判断したため close する"
            "（ダッシュボードの記録係が `req:close` を付けた。本人の発言は上のコメントを参照）。"
            "間違いなら reopen すればよい（`req:close` は外してある）。",
        )
        print(f"🙆 closed(requested) #{i['number']}: {i['title']}")
        out.append({"issue_id": str(i["number"]), "title": i["title"], "close_kind": "requested"})
    return out


METRIC_COMMENT_PREFIX = "## 指標の推移"


def render_metric_comment(e: dict, days: int = 14) -> str:
    vals = daily_values(e.get("history"))[-days:]
    head = f"{METRIC_COMMENT_PREFIX}（直近{len(vals)}日）\n\n"
    if not vals:
        return head + "まだ値がありません。\n"
    (d0, v0), (d1, v1) = vals[0], vals[-1]
    trend = f"{v0} ({d0}) → {v1} ({d1})"
    if v0:
        trend += f" {((v1 - v0) / abs(v0) * 100):+.1f}%"
    target = e.get("target_value")
    rows = "\n".join(f"| {d} | {v} |" for d, v in vals)
    return (
        head
        + f"{trend}\n\n目標: {target}（{e.get('target_direction')}）"
        + f" / 目標の連続達成: {target_streak(e)}日 / status: {e.get('status')}\n\n"
        + "| 日付 | 値 |\n|---|---|\n"
        + rows
        + "\n\n<sub>日次フローが評価するたびにこのコメントを上書きします（毎日追記すると読めなくなるため）。"
        + "同じ日に複数回評価された場合は最後の値を採っています。</sub>"
    )


def sync_metric_comments(evaluated: list[dict]) -> None:
    """評価した仮説の「指標の推移」コメントを最新にする。

    ★以前はこのコメントを更新する処理が存在しなかった★
    9/3 の GitHub 移行時に1度だけ書かれ、本文には「日次フローが評価するたびに更新されます」
    とあるのに実装が無く、9/1 の値のまま止まっていた（#7 は実際には 9/9 以降ずっと
    目標以下なのに、チケット上は 58分/日 に見えていた）。
    失敗しても日次FBは止めない（1件ずつ握りつぶしてログに残す）。
    """
    for e in evaluated:
        if e.get("status") not in EVALUATED_STATUSES and e.get("status") != "closed":
            continue
        try:
            gt.upsert_comment(int(e["issue_id"]), METRIC_COMMENT_PREFIX, render_metric_comment(e))
        except Exception as ex:  # noqa: BLE001
            print(f"⚠️ #{e['issue_id']} の指標コメント更新に失敗: {ex}")


def revive_untested() -> list[dict]:
    """untested の仮説に「打った手」が紐づいたら testing に戻す。

    untested を墓場にしないための対。
    「試す価値はあるが試していない」状態から、介入を打った瞬間に検証中へ復帰させる。
    復帰時に baseline を測り直すのが要点で、これをしないと介入前の古い baseline と
    比較して効果を誤判定する。
    """
    revived = []
    rows = gt._call(
        f"/repos/{gt.REPO}/issues?labels=life:hypothesis&state=open&per_page=100"
    )
    for i in rows or []:
        if gt._label_value(i, "status:") != "untested":
            continue
        actions = gt.load_actions_for(i["number"])
        if not actions:
            continue
        metric = gt.parse_metric(i.get("body") or "")
        if not metric:
            continue

        eval_date = datetime.datetime.now(JST).strftime("%Y-%m-%d")
        value, error = evaluate_metric(metric, eval_date)
        if value is None:
            print(f"⚠️ #{i['number']}: 復帰したいが metric が評価できない（{error}）")
            continue

        first = min(a["created_at"] for a in actions)[:10]
        gt.set_baseline(
            i["number"],
            value,
            f"[auto] 打った手が紐づいたため untested → testing に復帰。"
            f"baseline を現時点の実測値 {value} に打ち直した（最初の介入: {first}）。"
            f"介入前の baseline と比べると、介入前の変動を効果として誤読するため。",
        )
        gt.set_status_label(i["number"], "testing")
        record_metric(str(i["number"]), eval_date, value, None, None)
        print(f"♻️ revived #{i['number']}: {i['title']} (baseline={value})")
        revived.append({"issue_id": str(i["number"]), "title": i["title"]})
    return revived


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
