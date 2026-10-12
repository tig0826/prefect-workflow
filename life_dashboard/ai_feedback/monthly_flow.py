"""月次（長期）AI 分析（2026-10-10 追加）。毎月1日の朝に、前の月までを振り返る。

日次・週次・月次の役割分担:
  日次 … 進行中の実験の実況
  週次 … 7日を俯瞰して、今週の実験（仮説と打ち手の候補）を決める
  月次 … **週次では見えないこと**だけを扱う。処方（新しい実験）は出さない
         ・数か月かけてゆっくり動いたもの、季節・曜日の癖
         ・打った手・ブロック変更が「定着したか / 効果が薄れたか」
         ・繰り返し現れる本人の傾向（仮説として。診断はしない）

結果は ai_feedback（slot='monthly', feedback_date=その月の末日）に保存し、
ダッシュボードの「長期トレンド」（/trends）に出す。メールでも送る。

★数字は UI の API（/api/trends）から受け取る★
週平均の取り方・計測の変更の目印は lib/trends.ts にだけ持つ。ここで集計し直すと、
画面と AI が別の数字を見ることになる。足りない切り口は run_sql で自分で掘らせる。
"""

from __future__ import annotations

import calendar
import datetime
import html
import json
import smtplib
import urllib.request
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from zoneinfo import ZoneInfo

from google import genai
from google.genai import types
from prefect import flow, task
from prefect.blocks.system import Secret
from prefect.variables import Variable

from ai_feedback import github_tickets as gt
from ai_feedback import issue_tracker as it
from ai_feedback import sql_tool
from ai_feedback.weekly_flow import _RUN_SQL_DECL, _extract_json
from common.retry_policy import STORAGE_RETRY
from common.trino_api import TrinoAPI

JST = ZoneInfo("Asia/Tokyo")
TRINO = TrinoAPI(host="trino.mynet", port=80, user="tig", catalog="iceberg")

MODEL = "gemini-pro-latest"
THINKING_BUDGET = 16384
MAX_TOOL_TURNS = 15
SLOT = "monthly"
DEFAULT_URL = "http://life.mynet"


def _base_url() -> str:
    return (Variable.get("life_dashboard_url", default=None) or DEFAULT_URL).rstrip("/")


def _month_range(month: str) -> tuple[str, str]:
    y, m = (int(x) for x in month.split("-"))
    return f"{y:04d}-{m:02d}-01", f"{y:04d}-{m:02d}-{calendar.monthrange(y, m)[1]:02d}"


# ─────────────────────────────────────────────────────────────
# コンテキスト
# ─────────────────────────────────────────────────────────────

def _monthly_table(trends: dict) -> list[dict]:
    """日ごとの値から月平均を作る。週平均だけだと LLM が長い流れを掴みにくいので添える。"""
    keys = [m["key"] for m in trends["metrics"]]
    by_month: dict[str, dict[str, list[float]]] = {}
    for d in trends["days"]:
        mo = d["date"][:7]
        bucket = by_month.setdefault(mo, {k: [] for k in keys})
        for k in keys:
            v = d["values"].get(k)
            if v is not None:
                bucket[k].append(v)
    out = []
    for mo in sorted(by_month):
        row: dict = {"month": mo}
        for k, vals in by_month[mo].items():
            row[k] = round(sum(vals) / len(vals), 2) if vals else None
            row[f"n_{k}"] = len(vals)
        out.append(row)
    return out


def _safe(fn, default):
    try:
        return fn()
    except Exception as e:  # noqa: BLE001
        print(f"⚠️  コンテキストの一部を取得できなかった: {e}")
        return default


@task(name="Build monthly context", **STORAGE_RETRY)
def build_monthly_context(month: str) -> dict:
    start, end = _month_range(month)
    url = f"{_base_url()}/api/trends?range=all&end={end}&fresh=1"
    with urllib.request.urlopen(url, timeout=180) as r:
        trends = json.loads(r.read().decode("utf-8"))

    weekly_fb = it._records(TRINO.execute_query(f"""
        SELECT CAST(feedback_date AS VARCHAR) AS week_end, messages
        FROM iceberg.life_gold.ai_feedback
        WHERE slot = 'weekly'
          AND feedback_date BETWEEN DATE '{start}' - INTERVAL '7' DAY AND DATE '{end}'
        ORDER BY feedback_date
    """))
    past_monthly = it._records(TRINO.execute_query(f"""
        SELECT CAST(feedback_date AS VARCHAR) AS month_end, messages, context_summary
        FROM iceberg.life_gold.ai_feedback
        WHERE slot = '{SLOT}' AND feedback_date < DATE '{start}'
        ORDER BY feedback_date DESC
        LIMIT 2
    """))

    return {
        "month": month,
        "period": {"data_start": trends["dataStart"], "end": end},
        "metrics": trends["metrics"],
        "monthly_averages": _monthly_table(trends),
        "weekly_averages": [
            {"week": w["weekStart"], "partial": w["partial"],
             **{k: (round(v, 2) if v is not None else None) for k, v in w["values"].items()},
             "n": w["n"]}
            for w in trends["weeks"]
        ],
        "markers": trends["markers"],
        "hypotheses_history": _safe(lambda: gt.load_hypotheses_history(days=400), []),
        "active_hypotheses": _safe(
            lambda: [{k: h.get(k) for k in ("number", "title", "status", "metric_name", "n_actions")}
                     for h in gt.load_active_hypotheses(statuses=("open", "testing", "untested", "verifying"))],
            [],
        ),
        "weekly_insights_this_month": [
            {"week_end": r["week_end"], "insights": json.loads(r["messages"] or "[]")} for r in weekly_fb
        ],
        "previous_monthly": [
            {"month_end": r["month_end"],
             "insights": json.loads(r["messages"] or "[]"),
             "traits": (json.loads(r["context_summary"] or "{}") or {}).get("traits")}
            for r in past_monthly
        ],
    }


# ─────────────────────────────────────────────────────────────
# 生成
# ─────────────────────────────────────────────────────────────

_MONTHLY_PROMPT = """\
あなたはユーザー専属のライフアナリストで、医学・心理学・カウンセリングの専門知識を持つ。
月に1回、前の月までの**長い期間**を振り返る分析を書きます。

## あなたの役割（週次との違い）

週次の分析が毎週「7日を俯瞰して今週の実験を決める」ことをしています（`weekly_insights_this_month`）。
**月次のあなたは、週次では見えないことだけを書いてください。** 週次の言い換えは禁止です。
新しい実験（処方）は出しません。

見るべきもの:
1. **ゆっくりした変化** … 数か月かけて上がった・下がったもの、季節や月による違い
2. **打った手の行く末** … `markers` の intervention（打った手・ブロック設定の変更）の前後で
   指標がどうなり、その後も**続いているか / 元に戻ったか / 別の逃げ場に移っただけか**
3. **本人の傾向** … 数か月のデータで繰り返し現れるパターン（例: 崩れる前に先行して動く指標、
   回復のきっかけ、曜日・月末の癖）。これは「仮説」として書く。**病名や診断は書かない**

## 使える道具

`run_sql` で Trino に読み取り専用SQLを投げられます。渡された週平均・月平均は出発点です。
主張する前に必ずSQLで確かめてください（最低3回は掘ること）。Web検索も使えます。

{schema_doc}

## 長期で比べるときの罠（厳守）

- **`markers` の measure（計測・分類の変更）の前後の段差を、生活の変化として読んではいけない。**
  例: 勉強（STUDY）は途中から個人開発（DEVELOP）と分けて計測し始めたので、それ以前の勉強は
  個人開発に入っている。体重は 2026-09 に取得元が変わり、しばらく欠測している。
- **n（記録日数）が違う期間の平均を直接比べない。** 体重は測った日だけの平均。
- 介入の効果を言うときは「同じ時期に他に何が変わったか」（季節、仕事の繁忙、別の介入）を確認する。
  介入の直後に下がって数週で戻ったものは「定着」ではない。
- BROWSING は中身が保証されない箱（過去問道場や入眠用の動画も入る）。増減を逃避の増減と読まない。
- 個人開発の時間が増えたことを成果として褒めない（業務からの置き換えのことがある）。
- 本人の第一目標は**業務時間・仕事のパフォーマンスの向上**。
- `previous_monthly` で既に述べた傾向は、変化が無ければ繰り返さない（変化したなら「前回からどう変わったか」を書く）。

## 出力形式（JSONのみ・他のテキスト一切不要）

{{
  "insights": [
    {{"type": "insight"|"warning"|"danger"|"positive"|"question",
      "message": "250文字以内。期間と数値の根拠を必ず含める"}}
  ],
  "interventions_review": [
    {{"label": "markers の label をそのまま、または短く",
      "verdict": "定着" | "効果が薄れた" | "効果なし" | "判断できない",
      "evidence": "前後の週平均と、その後の推移。120文字以内"}}
  ],
  "traits": [
    {{"trait": "本人の傾向（仮説）。60文字以内",
      "evidence": "何か月分のどのデータで繰り返し見えたか。120文字以内"}}
  ],
  "next_month_focus": "来月、長い目で気にかけておくとよいこと1つ（実験ではなく、見守る観点）。100文字以内"
}}

insights は3〜5件。interventions_review は主要なもの（多くて6件。同じ系統のブロック変更はまとめてよい）。
traits は0〜3件（確信が持てなければ0件でよい）。

## データ
{data}
"""


@task(name="Generate monthly analysis", retries=1, retry_delay_seconds=60)
def generate_monthly(ctx: dict, api_key: str) -> dict:
    client = genai.Client(api_key=api_key)
    prompt = _MONTHLY_PROMPT.format(
        schema_doc=sql_tool.SCHEMA_DOC,
        data=json.dumps(ctx, ensure_ascii=False, indent=1, default=str),
    )
    config = types.GenerateContentConfig(
        tools=[types.Tool(function_declarations=[_RUN_SQL_DECL]), types.Tool(google_search=types.GoogleSearch())],
        # 組み込みツールと自前の関数呼び出しの併用に必須（weekly_flow.py 参照）
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


@task(name="Save monthly feedback", **STORAGE_RETRY)
def save_monthly(month_end: str, analysis: dict) -> None:
    import pandas as pd

    TRINO.execute_action(
        f"DELETE FROM iceberg.life_gold.ai_feedback "
        f"WHERE feedback_date = DATE '{month_end}' AND slot = '{SLOT}'"
    )
    df = pd.DataFrame([{
        "feedback_date": datetime.date.fromisoformat(month_end),
        "slot": SLOT,
        "generated_at": datetime.datetime.now(JST).replace(tzinfo=None),
        "messages": json.dumps(analysis.get("insights") or [], ensure_ascii=False),
        "model": MODEL,
        # 文脈（数十KB）は保存しない。画面に出す部分と、使ったSQLだけ
        "context_summary": json.dumps({
            "interventions_review": analysis.get("interventions_review") or [],
            "traits": analysis.get("traits") or [],
            "next_month_focus": analysis.get("next_month_focus"),
            "queries_executed": analysis.get("_queries_executed"),
        }, ensure_ascii=False, default=str),
    }])
    TRINO.insert_table("ai_feedback", "life_gold", df)
    print(f"✅ 月次分析を保存: {month_end} insights={len(analysis.get('insights') or [])}")


# ─────────────────────────────────────────────────────────────
# メール（週次レポートと同じ設定を使う。weekly_report_mail.py 参照）
# ─────────────────────────────────────────────────────────────

def render_monthly_html(month: str, analysis: dict, link: str) -> str:
    e = html.escape
    p = []
    p.append(f'<div style="font-size:18px;font-weight:bold">長期の振り返り（{e(month)} まで）</div>')
    p.append(f'<div style="margin:6px 0 14px"><a href="{e(link)}" style="font-size:12px;color:#2a78d6">'
             f'長期トレンドを開く（グラフはこちら）</a></div>')
    for it_ in analysis.get("insights") or []:
        p.append(f'<div style="font-size:13px;line-height:1.6;margin:6px 0;padding:8px 10px;'
                 f'border-left:3px solid #c3c2b7;background:#f7f7f5">{e(it_.get("message") or "")}</div>')
    rv = analysis.get("interventions_review") or []
    if rv:
        p.append('<div style="font-size:14px;font-weight:bold;margin:18px 0 6px">打った手はどうなったか</div>')
        for x in rv:
            p.append(f'<div style="font-size:12px;margin:4px 0"><b>［{e(str(x.get("verdict", "")))}］</b> '
                     f'{e(str(x.get("label", "")))}<br><span style="color:#52514e">{e(str(x.get("evidence", "")))}</span></div>')
    tr = analysis.get("traits") or []
    if tr:
        p.append('<div style="font-size:14px;font-weight:bold;margin:18px 0 6px">データから見えてきた傾向（仮説）</div>')
        for x in tr:
            p.append(f'<div style="font-size:12px;margin:4px 0">{e(str(x.get("trait", "")))}<br>'
                     f'<span style="color:#52514e">{e(str(x.get("evidence", "")))}</span></div>')
    if analysis.get("next_month_focus"):
        p.append(f'<div style="font-size:12px;margin-top:16px"><b>来月の焦点:</b> {e(str(analysis["next_month_focus"]))}</div>')
    return ('<!doctype html><html><body style="margin:0;background:#fff">'
            '<div style="max-width:640px;margin:0 auto;padding:16px;font-family:-apple-system,\'Hiragino Sans\','
            f'\'Noto Sans JP\',sans-serif;color:#0b0b0b">{"".join(p)}'
            '<div style="font-size:10px;color:#898781;margin-top:24px">life_dashboard の月次フローから自動送信</div>'
            '</div></body></html>')


@task(name="Send monthly mail", retries=2, retry_delay_seconds=60)
def send_monthly_mail(month: str, analysis: dict) -> None:
    address = Variable.get("report_mail_address", default=None)
    if not address:
        raise RuntimeError("Prefect Variable report_mail_address が未設定")
    password = "".join(Secret.load("gmail-app-password").get().split())
    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"長期の振り返り（{month} まで）"
    msg["From"] = address
    msg["To"] = address
    link = f"{_base_url()}/trends"
    msg.attach(MIMEText(f"長期の振り返り（{month} まで）\n{link}", "plain", "utf-8"))
    msg.attach(MIMEText(render_monthly_html(month, analysis, link), "html", "utf-8"))
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=60) as s:
        s.login(address, password)
        s.send_message(msg)
    print(f"📧 月次の振り返りを送信 → {address}")


@flow(name="AI Feedback Monthly", log_prints=True)
def ai_feedback_monthly_flow(month: str | None = None, force: bool = False, send_mail: bool = True):
    """月次の長期分析。month は 'YYYY-MM'（省略時は前の月）。"""
    if month is None:
        first = datetime.datetime.now(JST).date().replace(day=1)
        month = (first - datetime.timedelta(days=1)).strftime("%Y-%m")
    _, month_end = _month_range(month)

    if not force:
        df = TRINO.execute_query(f"""
            SELECT count(*) AS n FROM iceberg.life_gold.ai_feedback
            WHERE feedback_date = DATE '{month_end}' AND slot = '{SLOT}'
        """)
        if int(df.iloc[0]["n"]) > 0:
            print(f"⏭  {month} の月次分析は既に生成済み。スキップ")
            return {"skipped": True}

    print(f"🤖 月次分析: month={month} model={MODEL}")
    api_key = Secret.load("google-generative-ai-api-key").get()
    ctx = build_monthly_context(month)
    print(f"   週 {len(ctx['weekly_averages'])}件 / 目印 {len(ctx['markers'])}件")
    analysis = generate_monthly(ctx, api_key)
    save_monthly(month_end, analysis)

    mail_sent = False
    if send_mail:
        try:
            send_monthly_mail(month, analysis)
            mail_sent = True
        except Exception as e:  # noqa: BLE001
            print(f"⚠️  月次のメール送信に失敗: {e}")

    return {
        "skipped": False,
        "insights": len(analysis.get("insights") or []),
        "interventions_review": len(analysis.get("interventions_review") or []),
        "traits": len(analysis.get("traits") or []),
        "queries_executed": len(analysis.get("_queries_executed") or []),
        "mail_sent": mail_sent,
    }


if __name__ == "__main__":
    ai_feedback_monthly_flow(force=True)
