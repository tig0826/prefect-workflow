"""課題チケット（GitHub Issues）を読み書きするレイヤ。

★なぜ Trino から GitHub に移したか★
  課題管理は `ai_feedback_issues` に置いていたが、本人がその中身を見られなかった。
  本人の言葉:「今の所あなたが課題とか色々言ってるのが何もわからない」。
  仮説を本人が読めて**直せる**必要がある（測り方が違えば SQL を直せる）。
  GitHub なら3層の親子・ラベル・コメントがそのまま使え、スマホからも見られる。

★層と所在★
  life:problem     … 実在する課題。事実だけ。解決するまで close しない
  life:hypothesis  … 原因の推測。**指標の定義（metric_sql）を本文に持つ**
  life:action      … 打った手。効果測定の基準日

  Trino に残すのは `ai_metric_history`（指標の時系列）だけ。
  時系列は行動ログや睡眠と JOIN して相関を出す必要があり、Issue のコメントには置けない。

★指標の定義の形式★
  本文に HTML コメントの key: value と、素の ```sql ブロックを置く。
  YAML パーサに頼らないのは、**本人が読んで直せること**を機械可読性より優先したため。
  崩れても人には読める。

      <!-- metric
      name: ゲーム時間（直近7日平均）
      unit: 分/日
      baseline: 137.3
      target: 60
      direction: decrease
      mention_count: 2
      -->

      ```sql
      SELECT ...
      ```

  機械が読むのは HTML コメント側。表は人が読む用の重複なので、
  食い違ったら HTML コメントを正とする。
"""

import os
import re

import httpx

REPO = os.environ.get("GITHUB_TICKET_REPO", "tig0826/life_dashboard_ui")
API = "https://api.github.com"


_TOKEN_CACHE: str | None = None


def _token() -> str:
    """GitHub のトークンを得る。

    優先順:
      1. 環境変数 GITHUB_TICKET_TOKEN（ローカルでの検証用）
      2. Prefect Secret ブロック `github-ticket-token`（k8s 上の flow はこちら）

    ★Prefect Secret を使う理由★
    flow は kubernetes work pool で動き、prefect.yaml の job_variables.env は
    平文の map なので秘密を書けない（このファイルはコミットされる）。
    このリポジトリでは Gemini の鍵も Secret ブロック経由なので同じ形に揃える。
    """
    global _TOKEN_CACHE
    if _TOKEN_CACHE:
        return _TOKEN_CACHE

    t = os.environ.get("GITHUB_TICKET_TOKEN", "")
    if not t:
        try:
            from prefect.blocks.system import Secret

            t = Secret.load("github-ticket-token").get()
        except Exception as e:  # noqa: BLE001
            raise RuntimeError(
                "GitHub のトークンが取得できません。課題チケットは GitHub にあるため必須です。"
                "環境変数 GITHUB_TICKET_TOKEN か、Prefect Secret ブロック "
                f"'github-ticket-token' を用意してください（{e}）"
            ) from e
    _TOKEN_CACHE = t
    return t


def _call(path: str, method: str = "GET", body: dict | None = None) -> object:
    r = httpx.request(
        method,
        f"{API}{path}",
        headers={
            "authorization": f"Bearer {_token()}",
            "accept": "application/vnd.github+json",
            "x-github-api-version": "2022-11-28",
        },
        json=body,
        timeout=30.0,
    )
    if r.status_code >= 400:
        raise RuntimeError(f"GitHub {r.status_code}: {r.text[:300]}")
    return None if r.status_code == 204 else r.json()


def _labels(issue: dict) -> list[str]:
    return [l["name"] if isinstance(l, dict) else l for l in issue.get("labels", [])]


def _label_value(issue: dict, prefix: str) -> str | None:
    for l in _labels(issue):
        if l.startswith(prefix):
            return l[len(prefix) :]
    return None


# ─────────────────────────────────────────────────────────────
# 指標定義の抽出
# ─────────────────────────────────────────────────────────────

_METRIC_BLOCK = re.compile(r"<!--\s*metric\s*(.*?)-->", re.DOTALL)
_SQL_BLOCK = re.compile(r"```sql\s*(.*?)```", re.DOTALL)


def parse_metric(body: str) -> dict:
    """本文から指標の定義を取り出す。

    返すキー: metric_name / metric_unit / baseline_value / target_value /
              target_direction / metric_sql / mention_count
    定義が無ければ空の dict（＝指標を持たない仮説。評価対象にしない）。
    """
    m = _METRIC_BLOCK.search(body or "")
    s = _SQL_BLOCK.search(body or "")
    if not m or not s:
        return {}

    kv: dict[str, str] = {}
    for line in m.group(1).splitlines():
        if ":" not in line:
            continue
        k, v = line.split(":", 1)
        kv[k.strip()] = v.strip()

    def num(key: str) -> float | None:
        try:
            return float(kv[key])
        except (KeyError, ValueError):
            return None

    direction = kv.get("direction", "")
    if direction not in ("increase", "decrease"):
        # 方向が無いと「動いた」を方向つきで判定できない。評価対象から外す。
        return {}

    return {
        "metric_name": kv.get("name") or None,
        "metric_unit": kv.get("unit") or None,
        "baseline_value": num("baseline"),
        "target_value": num("target"),
        "target_direction": direction,
        "metric_sql": s.group(1).strip(),
        "mention_count": int(num("mention_count") or 0),
    }


def _set_metric_field(body: str, key: str, value: str) -> str:
    """本文の <!-- metric --> 内の1項目を書き換える。

    ★本文の他の部分には触らない★
    本文は本人が書く場所で、機械が全体を書き換えると
    「本人が書いた前提」と「機械が上書きした前提」が区別できなくなる。
    """
    m = _METRIC_BLOCK.search(body)
    if not m:
        return body
    inner = m.group(1)
    if re.search(rf"^{re.escape(key)}\s*:", inner, re.MULTILINE):
        inner_new = re.sub(
            rf"^{re.escape(key)}\s*:.*$", f"{key}: {value}", inner, flags=re.MULTILINE
        )
    else:
        inner_new = inner.rstrip() + f"\n{key}: {value}\n"
    return body[: m.start(1)] + inner_new + body[m.end(1) :]


# ─────────────────────────────────────────────────────────────
# 読み取り
# ─────────────────────────────────────────────────────────────


def load_problems() -> list[dict]:
    """実在する課題（life:problem）。severity 付き。

    ★これが「課題」。仮説と混同しないこと。★
    実害の例: 「一番重い課題は？」に対して仮説層を読んで答え、
    sev:S1 ではない別のものを最重要と報告した事故がある。
    """
    rows = _call(f"/repos/{REPO}/issues?labels=life:problem&state=open&per_page=100")
    out = []
    for i in rows:  # type: ignore[union-attr]
        if i.get("pull_request"):
            continue
        out.append(
            {
                "number": i["number"],
                "title": i["title"],
                "body": i.get("body") or "",
                "severity": _label_value(i, "sev:"),
                "priority": _label_value(i, "pri:"),
                "size": _label_value(i, "size:"),
                "status": _label_value(i, "status:") or "open",
            }
        )
    # S1 が先、次に P0
    return sorted(out, key=lambda x: (x["severity"] or "S9", x["priority"] or "P9"))


def load_active_hypotheses(statuses: tuple[str, ...] = ("open", "testing")) -> list[dict]:
    """評価対象の仮説（life:hypothesis で open かつ status が statuses のどれか）。

    既定は open/testing（＝日次で実況する対象）。
    `untested` は「介入を打っていないので未検証」であり含めない
    （繰り返しを止めるため）。ただし棄却とは意味が違う。

    ★2026-10-03: statuses を引数にした★
    verifying（目標を連続達成して実況から外れた仮説）も指標の評価だけは続けないと、
    「安定したので閉じる」「後戻りしたので testing に戻す」が判定できない。
    実況の対象と評価の対象を分けるために、呼び出し側で選ばせる。
    """
    rows = _call(f"/repos/{REPO}/issues?labels=life:hypothesis&state=open&per_page=100")
    parents = parent_map()
    out = []
    for i in rows:  # type: ignore[union-attr]
        if i.get("pull_request"):
            continue
        status = _label_value(i, "status:") or "open"
        if status not in statuses:
            continue
        metric = parse_metric(i.get("body") or "")
        if not metric:
            # 指標を持たない仮説は評価できない。評価対象から外す。
            continue
        out.append(
            {
                "number": i["number"],
                "title": i["title"],
                "body": i.get("body") or "",
                "status": status,
                "parent": parents.get(i["number"]),
                "action_candidate": parse_action_candidate(i.get("body") or ""),
                "n_actions": _count_children(i),
                **metric,
            }
        )
    return out


_ACTION_CANDIDATE = re.compile(r"^## 打ち手の候補\s*\n(.*?)(?=^## |\Z)", re.DOTALL | re.MULTILINE)


def parse_action_candidate(body: str) -> str | None:
    """本文の「## 打ち手の候補」節を取り出す（週次が起票時に書く）。無ければ None。"""
    m = _ACTION_CANDIDATE.search(body or "")
    if not m:
        return None
    # 本文に添えている案内（<sub>…</sub>）は候補そのものではないので落とす
    text = "\n".join(l for l in m.group(1).splitlines() if not l.strip().startswith("<sub>")).strip()
    return text[:400] or None


def _count_children(issue: dict) -> int:
    """仮説の子（＝打った手）の数。一覧 API の sub_issues_summary を使い、無ければ1件ずつ引く。"""
    summary = issue.get("sub_issues_summary")
    if isinstance(summary, dict) and "total" in summary:
        return int(summary.get("total") or 0)
    try:
        return len(_call(f"/repos/{REPO}/issues/{issue['number']}/sub_issues") or [])  # type: ignore[arg-type]
    except Exception:
        return 0


def load_hypotheses_history(days: int = 180) -> list[dict]:
    """「もう扱った」仮説の一覧。週次が同じ仮説を出し直さないために渡す。

    ★なぜ必要か★
    週次の重複チェックは active（open/testing）しか見ていなかった。
    untested・verifying・abandoned・close 済みは見えないので、
    検証が済んだ仮説や放置中の仮説と同じものを新規に起票しうる。

    返すもの: open のうち status が untested/verifying/abandoned のもの +
              直近 days 日以内に close されたもの。
    """
    import datetime as _dt

    out = []
    rows = _call(f"/repos/{REPO}/issues?labels=life:hypothesis&state=open&per_page=100")
    for i in rows or []:  # type: ignore[union-attr]
        if i.get("pull_request"):
            continue
        status = _label_value(i, "status:") or "open"
        if status in ("untested", "verifying", "abandoned"):
            out.append({"number": i["number"], "title": i["title"], "state": "open",
                        "status": status, "closed_reason": None, "closed_at": None})

    cutoff = _dt.datetime.now(_dt.timezone.utc) - _dt.timedelta(days=days)
    rows = _call(
        f"/repos/{REPO}/issues?labels=life:hypothesis&state=closed&sort=updated&direction=desc&per_page=100"
    )
    for i in rows or []:  # type: ignore[union-attr]
        if i.get("pull_request") or not i.get("closed_at"):
            continue
        closed_at = _dt.datetime.fromisoformat(i["closed_at"].replace("Z", "+00:00"))
        if closed_at < cutoff:
            continue
        out.append({"number": i["number"], "title": i["title"], "state": "closed",
                    "status": _label_value(i, "status:"),
                    "closed_reason": i.get("state_reason"),
                    "closed_at": i["closed_at"][:10]})
    return out


def load_recently_verified(days: int = 60) -> list[dict]:
    """検証完了で close した仮説（status:verified）のうち、直近 days 日に閉じたもの。

    close した後も一定期間は指標を見張り、後戻りしたら再開するために使う。
    本人の close 依頼で閉じたもの（status:verified を持たない）は見張らない。
    本人の判断を機械が覆さないため。
    """
    import datetime as _dt

    cutoff = _dt.datetime.now(_dt.timezone.utc) - _dt.timedelta(days=days)
    rows = _call(
        f"/repos/{REPO}/issues?labels=life:hypothesis,status:verified&state=closed&per_page=100"
    )
    out = []
    for i in rows or []:  # type: ignore[union-attr]
        if i.get("pull_request") or not i.get("closed_at"):
            continue
        if _dt.datetime.fromisoformat(i["closed_at"].replace("Z", "+00:00")) < cutoff:
            continue
        metric = parse_metric(i.get("body") or "")
        if not metric:
            continue
        out.append({"number": i["number"], "title": i["title"],
                    "n_actions": _count_children(i), **metric})
    return out


def parent_map() -> dict[int, int]:
    """子 → 親 の対応表を作る。

    ★REST の issue GET は `parent` を返さない★
    実測: #13 は #22 の sub-issue なのに `GET /issues/13` の `parent` は null
    （`sub_issues_summary` は返る）。逆方向（親から子）は取れるので、
    課題ごとに sub_issues を引いて対応表を組む。
    """
    m: dict[int, int] = {}
    for p in load_problems():
        try:
            subs = _call(f"/repos/{REPO}/issues/{p['number']}/sub_issues")
        except Exception:
            continue
        for s in subs or []:  # type: ignore[union-attr]
            m[s["number"]] = p["number"]
    return m


def load_actions_for(number: int) -> list[dict]:
    """その仮説に対して打った手（子の life:action）。

    ★これが無いと「未検証」と「反証」を区別できない★
    指標が動かない理由は3つある: 仮説が違う / 有効な介入を打っていない /
    介入は打ったが守られていない。介入の有無を見ずに棄却すると全部1つ目にしてしまう。
    """
    try:
        subs = _call(f"/repos/{REPO}/issues/{number}/sub_issues")
    except Exception:
        return []
    out = []
    for s in subs or []:  # type: ignore[union-attr]
        detail = _call(f"/repos/{REPO}/issues/{s['number']}")
        out.append(
            {
                "number": s["number"],
                "title": s["title"],
                "kind": _label_value(detail, "kind:") or "",  # type: ignore[arg-type]
                "state": s["state"],
                "created_at": detail["created_at"],  # type: ignore[index]
            }
        )
    return out


# ─────────────────────────────────────────────────────────────
# 書き込み
# ─────────────────────────────────────────────────────────────


def comment(number: int, body: str) -> None:
    _call(f"/repos/{REPO}/issues/{number}/comments", "POST", {"body": body})


def set_status_label(number: int, status: str, note: str | None = None) -> None:
    """status:* ラベルを張り替える。close はしない。

    ★close しない理由★
    課題（Problem）は解決するまで閉じない。仮説が反証されても親の課題は生きている。
    仮説側は close してよいが、それは呼び出し側（daily_flow）の判断に任せる。
    """
    cur = _call(f"/repos/{REPO}/issues/{number}")
    for l in _labels(cur):  # type: ignore[arg-type]
        if l.startswith("status:") and l != f"status:{status}":
            try:
                _call(f"/repos/{REPO}/issues/{number}/labels/{l}", "DELETE")
            except Exception:
                pass
    _call(f"/repos/{REPO}/issues/{number}/labels", "POST", {"labels": [f"status:{status}"]})
    if note:
        comment(number, note)


CLOSE_REQUEST_LABEL = "req:close"


def close_issue(number: int, reason: str, note: str | None = None) -> None:
    """issue を閉じる。**日次フローだけが呼ぶ。**

    reason: "completed"（解決・検証完了・本人の判断）/ "not_planned"（棄却）

    ★close の権限★
    close できるのは人間・CLI と日次フローだけ。ダッシュボード（チャットと司書）は
    close を実装しない。司書にできるのは「本人が閉じてよいと言った」という依頼を
    `req:close` ラベルで残すことまでで、実行は日次フローが process_close_requests で行う。
    「チャットは提案、パイプラインが決定」という分担を崩さずに、本人の決定を確実に通すため。

    閉じるときは `req:close` を外す。外さないと、本人が reopen した翌朝に
    また自動で閉じてしまう。
    """
    if reason not in ("completed", "not_planned"):
        raise ValueError(f"reason は completed / not_planned のみ: {reason!r}")
    if note:
        comment(number, note)
    try:
        _call(f"/repos/{REPO}/issues/{number}/labels/{CLOSE_REQUEST_LABEL}", "DELETE")
    except Exception:
        pass  # 付いていなければ 404。問題ない
    _call(
        f"/repos/{REPO}/issues/{number}",
        "PATCH",
        {"state": "closed", "state_reason": reason},
    )


def reopen_issue(number: int, status: str, note: str) -> None:
    """閉じた issue を開け直す（日次フローの後戻り検知だけが呼ぶ）。"""
    comment(number, note)
    _call(f"/repos/{REPO}/issues/{number}", "PATCH", {"state": "open"})
    set_status_label(number, status)


def load_close_requests() -> list[dict]:
    """本人が「閉じてよい」と言った（司書が req:close を付けた）open の issue。種類は問わない。"""
    rows = _call(
        f"/repos/{REPO}/issues?labels={CLOSE_REQUEST_LABEL}&state=open&per_page=100"
    )
    return [
        {"number": i["number"], "title": i["title"], "labels": _labels(i)}
        for i in rows or []  # type: ignore[union-attr]
        if not i.get("pull_request")
    ]


def upsert_comment(number: int, prefix: str, body: str) -> None:
    """本文が prefix で始まるコメントがあれば書き換え、無ければ新しく足す。

    ★毎日コメントを足さない★
    指標の推移のように毎日変わるものを追記で積むと、コメント欄が数字で埋まって
    本人にも司書にも読めなくなる。1つのコメントを上書きし続ける。
    """
    rows = _call(f"/repos/{REPO}/issues/{number}/comments?per_page=100")
    for c in rows or []:  # type: ignore[union-attr]
        if str(c.get("body") or "").startswith(prefix):
            if c.get("body") != body:
                _call(f"/repos/{REPO}/issues/comments/{c['id']}", "PATCH", {"body": body})
            return
    comment(number, body)


def bump_mention(number: int) -> int:
    """言及回数を1つ増やして返す。

    ★カウンタを Trino に置かない★
    課題管理を GitHub に一本化した以上、カウンタだけ別の場所に置くと
    「同じ情報の出どころが2つ」に戻る。本文の metric ブロックに持たせる。
    """
    issue = _call(f"/repos/{REPO}/issues/{number}")
    body = issue.get("body") or ""  # type: ignore[union-attr]
    n = parse_metric(body).get("mention_count", 0) + 1
    _call(
        f"/repos/{REPO}/issues/{number}",
        "PATCH",
        {"body": _set_metric_field(body, "mention_count", str(n))},
    )
    return n


def set_baseline(number: int, value: float, note: str) -> None:
    """baseline を打ち直す（untested から復帰するときなど）。

    介入前の baseline と比べると、介入前の変動を効果と誤読する。
    """
    issue = _call(f"/repos/{REPO}/issues/{number}")
    body = issue.get("body") or ""  # type: ignore[union-attr]
    _call(
        f"/repos/{REPO}/issues/{number}",
        "PATCH",
        {"body": _set_metric_field(body, "baseline", str(value))},
    )
    comment(number, note)
