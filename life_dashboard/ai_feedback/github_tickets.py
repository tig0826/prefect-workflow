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


def load_active_hypotheses() -> list[dict]:
    """検証中の仮説（life:hypothesis で open かつ status が open/testing）。

    `untested` は「介入を打っていないので未検証」であり active に含めない
    （繰り返しを止めるため）。ただし棄却とは意味が違う。
    """
    rows = _call(f"/repos/{REPO}/issues?labels=life:hypothesis&state=open&per_page=100")
    parents = parent_map()
    out = []
    for i in rows:  # type: ignore[union-attr]
        if i.get("pull_request"):
            continue
        status = _label_value(i, "status:") or "open"
        if status not in ("open", "testing"):
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
                **metric,
            }
        )
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
