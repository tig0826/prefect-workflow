"""週次レポートをメールで送る（2026-10-10 追加）。

ダッシュボードの /report（週次レポート）と同じ内容を、グラフ画像つきの HTML メールにして
Gmail から本人宛てに送る。週次フロー（weekly_flow.py）の最後に呼ばれる。

★集計を Python で書き直さない★
数字は UI の API（/api/report/weekly）から JSON で受け取る。集計を2か所に持つと
ページとメールで数字がずれるため（平均の取り方の決まりは lib/weekly-report.ts にだけある）。
ここでやるのは見た目（HTML とグラフ画像）だけ。

設定（Prefect）:
  Secret   gmail-app-password   … Gmail のアプリパスワード（16文字。2段階認証が必要）
  Variable report_mail_address  … 送信元＝宛先の Gmail アドレス
  Variable life_dashboard_url   … 省略時 http://life.mynet
"""

from __future__ import annotations

import datetime
import html
import io
import json
import smtplib
import urllib.request
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from prefect import flow, task  # noqa: E402

DEFAULT_URL = "http://life.mynet"

# ── 配色（白い面に対して検証済み: 明度帯・彩度・CVD ΔE≥8）──
# ページ（暗い面）と同じ色相の明るい面向けの段。白地で 3:1 を切る色があるので、
# 数字はグラフだけに頼らず必ず表でも出している。
C = {
    "work": "#1baf7a",
    "study": "#eda100",
    "dev": "#2a78d6",
    "leisure": "#e87ba4",
    "browsing": "#6250d6",
    "media": "#e87ba4",
    "game": "#6250d6",
    "manga": "#eb6834",
    "social": "#2a78d6",
    "ink": "#0b0b0b",
    "ink2": "#52514e",
    "muted": "#898781",
    "grid": "#e1e0d9",
    "axis": "#c3c2b7",
    "good": "#006300",
    "bad": "#b4441c",
}
DOW = "月火水木金土日"


def _md(d: str) -> str:
    return f"{int(d[5:7])}/{int(d[8:10])}"


def _day_label(d: str) -> str:
    return f"{_md(d)}({DOW[datetime.date.fromisoformat(d).weekday()]})"


def _fmt(v, digits: int) -> str:
    if v is None:
        return "–"
    return f"{v:,.{digits}f}"


# ─────────────────────────────────────────────────────────────
# 取得
# ─────────────────────────────────────────────────────────────

def fetch_report(base_url: str, week_end: str | None) -> dict:
    q = f"?fresh=1" + (f"&week_end={week_end}" if week_end else "")
    with urllib.request.urlopen(f"{base_url}/api/report/weekly{q}", timeout=90) as r:
        return json.loads(r.read().decode("utf-8"))


# ─────────────────────────────────────────────────────────────
# グラフ
# ─────────────────────────────────────────────────────────────

def _setup_axes(ax, days: list[dict], week_start: str):
    xs = list(range(len(days)))
    ax.set_xticks(xs)
    ax.set_xticklabels([_md(d["date"]) for d in days], fontsize=7, color=C["muted"])
    ax.tick_params(axis="y", labelsize=7, colors=C["muted"], length=0)
    ax.tick_params(axis="x", length=0)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(C["axis"])
    ax.grid(axis="y", color=C["grid"], linewidth=0.6)
    ax.set_axisbelow(True)
    # 前週と今週の境目
    idx = next((i for i, d in enumerate(days) if d["date"] >= week_start), None)
    if idx is not None:
        ax.axvline(idx - 0.5, color=C["muted"], linestyle=(0, (3, 3)), linewidth=0.8)
        ax.text(idx - 0.4, 1.0, "今週 →", transform=ax.get_xaxis_transform(),
                fontsize=7, color=C["muted"], va="top")
    return xs


def _fig(title: str, sub: str | None = None):
    # 上から タイトル → 説明 → 凡例 → グラフ の順に積む（凡例が説明文に重ならないように）
    fig, ax = plt.subplots(figsize=(6.0, 2.7), dpi=150)
    fig.subplots_adjust(left=0.07, right=0.99, top=0.70, bottom=0.10)
    fig.text(0.07, 0.97, title, fontsize=10, fontweight="bold", color=C["ink"], va="top")
    if sub:
        fig.text(0.07, 0.885, sub, fontsize=7, color=C["ink2"], va="top")
    return fig, ax


def _legend(ax, items: list[tuple[str, str]]):
    from matplotlib.patches import Patch

    ax.figure.legend(
        handles=[Patch(color=c, label=l) for l, c in items],
        loc="upper left", bbox_to_anchor=(0.06, 0.83), ncol=len(items), frameon=False,
        fontsize=7, handlelength=1.0, handleheight=0.8, columnspacing=1.0, labelcolor=C["ink2"],
    )


def _png(fig) -> bytes:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", facecolor="white")
    plt.close(fig)
    return buf.getvalue()


def _stacked(days, week_start, series, title, sub, line=None) -> bytes:
    fig, ax = _fig(title, sub)
    xs = _setup_axes(ax, days, week_start)
    bottom = [0.0] * len(days)
    for key, label, color in series:
        vals = [d.get(key) or 0 for d in days]
        # 積み上げの境目に白い隙間を入れる（色だけに頼らない）
        ax.bar(xs, vals, bottom=bottom, color=color, width=0.72, edgecolor="white", linewidth=0.8, label=label)
        bottom = [b + v for b, v in zip(bottom, vals)]
    items = [(l, c) for _, l, c in series]
    if line:
        key, label, color = line
        ys = [d.get(key) for d in days]
        ax.plot([x for x, y in zip(xs, ys) if y is not None], [y for y in ys if y is not None],
                color=color, linestyle=(0, (4, 3)), linewidth=1.4, marker="o", markersize=3)
        items.append((label, color))
    _legend(ax, items)
    return _png(fig)


def render_charts(rep: dict) -> list[tuple[str, bytes]]:
    plt.rcParams["font.family"] = ["Noto Sans CJK JP", "sans-serif"]
    days = [d for d in rep["days"]]
    ws = rep["weekStart"]
    charts: list[tuple[str, bytes]] = []

    charts.append(("time", _stacked(
        days, ws,
        [("workH", "業務", C["work"]), ("studyH", "勉強", C["study"]), ("devH", "個人開発", C["dev"]),
         ("leisureH", "娯楽", C["leisure"]), ("browsingH", "ブラウジング", C["browsing"])],
        "時間の使い方", "1日ごとの時間（h）。点線の右が今週",
    )))
    charts.append(("leisure", _stacked(
        days, ws,
        [("mediaH", "動画", C["media"]), ("gameH", "ゲーム", C["game"]),
         ("mangaH", "漫画", C["manga"]), ("socialH", "SNS", C["social"])],
        "娯楽の内訳", "h。点線は21時以降の分",
        line=("leisureAfter21H", "うち21時以降", C["ink2"]),
    )))

    # 睡眠
    fig, ax = _fig("睡眠", "昼寝を含む（h）。横線は7時間")
    xs = _setup_axes(ax, days, ws)
    ax.bar(xs, [d.get("sleepH") or 0 for d in days], color=C["dev"], width=0.72)
    ax.axhline(7, color=C["ink2"], linewidth=0.8, alpha=0.6)
    charts.append(("sleep", _png(fig)))

    # 体重（7日平均が主、当日は点）
    fig, ax = _fig("体重", "太線が7日平均、点がその日の体重（kg）")
    xs = _setup_axes(ax, days, ws)
    w7 = [(x, d.get("weight7d")) for x, d in zip(xs, days) if d.get("weight7d") is not None]
    wd = [(x, d.get("weight")) for x, d in zip(xs, days) if d.get("weight") is not None]
    if w7:
        ax.plot(*zip(*w7), color=C["study"], linewidth=2.2)
    if wd:
        ax.scatter(*zip(*wd), color=C["ink2"], s=10, zorder=3)
    ax.yaxis.set_major_formatter(matplotlib.ticker.FormatStrFormatter("%.1f"))
    _legend(ax, [("7日平均", C["study"]), ("当日", C["ink2"])])
    charts.append(("weight", _png(fig)))

    # 安静時心拍
    fig, ax = _fig("安静時心拍", "bpm。下がるほど回復している目安")
    xs = _setup_axes(ax, days, ws)
    hr = [(x, d.get("restingHr")) for x, d in zip(xs, days) if d.get("restingHr") is not None]
    if hr:
        ax.plot(*zip(*hr), color=C["leisure"], linewidth=1.8, marker="o", markersize=3)
    charts.append(("rhr", _png(fig)))

    # 解錠
    fig, ax = _fig("スマホの解錠回数", "回/日")
    xs = _setup_axes(ax, days, ws)
    ax.bar(xs, [d.get("unlocks") or 0 for d in days], color=C["browsing"], width=0.72)
    charts.append(("unlocks", _png(fig)))
    return charts


# ─────────────────────────────────────────────────────────────
# HTML
# ─────────────────────────────────────────────────────────────

TYPE_LABEL = {"positive": "良い変化", "warning": "注意", "danger": "要注意", "insight": "気づき", "question": "質問"}
KIND_LABEL = {"problem": "課題", "hypothesis": "仮説", "action": "打った手"}


def _metric_row(m: dict) -> str:
    this, prev, digits = m.get("this"), m.get("prev"), m["digits"]
    delta = None if this is None or prev is None else round(this - prev, digits)
    if delta is None or delta == 0:
        verdict, arrow = "", "→" if delta == 0 else ""
    else:
        up = delta > 0
        arrow = "↑" if up else "↓"
        better = m.get("better")
        verdict = "" if better is None else ("改善" if (better == "up") == up else "悪化")
    color = C["good"] if verdict == "改善" else C["bad"] if verdict == "悪化" else C["ink2"]
    delta_s = "–" if delta is None else f"{'+' if delta > 0 else ''}{_fmt(delta, digits)}"
    n_note = ""
    if m.get("basis") == "data" and (m["nThis"] != m["nPrev"] or m["nThis"] < 4):
        n_note = f'<div style="font-size:10px;color:{C["muted"]}">記録 {m["nThis"]}日 / 前週 {m["nPrev"]}日</div>'
    elif m.get("basis") == "workday" and (m["nThis"] != 5 or m["nPrev"] != 5):
        n_note = f'<div style="font-size:10px;color:{C["muted"]}">勤務日 {m["nThis"]}日 / 前週 {m["nPrev"]}日で平均</div>'
    return (
        f'<tr><td style="padding:5px 6px;border-bottom:1px solid #eee">{html.escape(m["label"])}{n_note}</td>'
        f'<td style="padding:5px 6px;border-bottom:1px solid #eee;text-align:right;font-weight:bold">{_fmt(this, digits)}'
        f'<span style="font-size:10px;color:{C["muted"]};font-weight:normal"> {html.escape(m["unit"])}</span></td>'
        f'<td style="padding:5px 6px;border-bottom:1px solid #eee;text-align:right;color:{C["ink2"]}">{_fmt(prev, digits)}</td>'
        f'<td style="padding:5px 6px;border-bottom:1px solid #eee;text-align:right;color:{color}">'
        f'{arrow} {delta_s} {verdict}</td></tr>'
    )


def render_html(rep: dict, base_url: str, chart_ids: list[str]) -> str:
    e = html.escape
    link = f"{base_url}/report?week_end={rep['weekEnd']}"
    period = f"{_day_label(rep['weekStart'])} 〜 {_day_label(rep['weekEnd'])}"
    parts: list[str] = []
    h2 = f'style="font-size:14px;margin:22px 0 8px;color:{C["ink"]}"'

    parts.append(
        f'<div style="font-size:18px;font-weight:bold;color:{C["ink"]}">週次レポート</div>'
        f'<div style="font-size:12px;color:{C["ink2"]};margin-top:2px">{e(period)}</div>'
        f'<div style="margin-top:8px"><a href="{e(link)}" style="font-size:12px;color:{C["dev"]}">'
        f'ダッシュボードで開く（グラフを触って確かめられます）</a></div>'
    )

    # 数字
    th = f'style="padding:5px 6px;text-align:right;font-weight:normal;color:{C["muted"]};font-size:11px"'
    rows = "".join(_metric_row(m) for m in rep["summary"])
    parts.append(
        f'<h2 {h2}>前週との比較</h2>'
        f'<table cellspacing="0" style="border-collapse:collapse;width:100%;font-size:12px;color:{C["ink"]}">'
        f'<tr><th style="padding:5px 6px;text-align:left;font-weight:normal;color:{C["muted"]};font-size:11px">指標</th>'
        f'<th {th}>今週</th><th {th}>前週</th><th {th}>差</th></tr>{rows}</table>'
        f'<div style="font-size:10px;color:{C["muted"]};margin-top:4px">'
        f'業務は勤務日（土日・祝日を除く）1日あたり。勉強・開発・娯楽は1日あたり（記録が無い日は0時間）。'
        f'睡眠・心拍などは記録がある日だけの平均。</div>'
    )

    # AI
    ai = rep.get("ai")
    parts.append(f'<h2 {h2}>AI の週次分析</h2>')
    if not ai:
        parts.append(f'<div style="font-size:12px;color:{C["ink2"]}">この週の週次分析はまだありません。</div>')
    else:
        for it in ai.get("insights") or []:
            label = TYPE_LABEL.get(it.get("type"), "気づき")
            parts.append(
                f'<div style="font-size:12px;line-height:1.6;margin:6px 0;padding:8px 10px;'
                f'border-left:3px solid {C["axis"]};background:#f7f7f5">'
                f'<b style="color:{C["ink2"]}">［{e(label)}］</b> {e(it.get("message") or "")}</div>'
            )
        exps = ai.get("experiments") or []
        if exps:
            parts.append(f'<div style="font-size:12px;font-weight:bold;margin:14px 0 4px">次の週に向けた仮説と打ち手の候補</div>')
            for x in exps:
                metric = ""
                if x.get("metric_name"):
                    metric = (f'<div style="font-size:10px;color:{C["muted"]}">{e(str(x["metric_name"]))}: '
                              f'{e(str(x.get("baseline_value", "?")))} → {e(str(x.get("target_value", "?")))} '
                              f'{e(str(x.get("metric_unit") or ""))}</div>')
                parts.append(
                    f'<div style="font-size:12px;line-height:1.6;margin:6px 0;padding:8px 10px;border:1px solid #e5e5e0;border-radius:6px">'
                    f'<b>{e(x.get("title") or "")}</b>'
                    + (f'<div style="color:{C["ink2"]}">{e(x["hypothesis"])}</div>' if x.get("hypothesis") else "")
                    + (f'<div>打ち手: {e(x["action_candidate"])}</div>' if x.get("action_candidate") else "")
                    + metric + "</div>"
                )
            parts.append(f'<div style="font-size:10px;color:{C["muted"]}">試すときはチャットで「#番号 を試す」と伝えると打った手として記録されます。</div>')

    # グラフ
    parts.append(f'<h2 {h2}>グラフ（前週＋今週の14日）</h2>')
    for cid in chart_ids:
        parts.append(f'<div style="margin:8px 0"><img src="cid:{cid}" width="600" '
                     f'style="width:100%;max-width:600px;height:auto;display:block" alt="{cid}"></div>')

    # チケット
    tickets = rep.get("tickets")
    parts.append(f'<h2 {h2}>この週のチケットの動き</h2>')
    if tickets is None:
        parts.append(f'<div style="font-size:12px;color:{C["ink2"]}">取得できませんでした（{e(str(rep.get("ticketsError")))}）</div>')
    else:
        def tlist(items, empty):
            if not items:
                return f'<div style="font-size:12px;color:{C["muted"]}">{empty}</div>'
            return "".join(
                f'<div style="font-size:12px;margin:3px 0"><span style="color:{C["muted"]}">#{t["number"]}</span> '
                f'［{KIND_LABEL.get(t["kind"], "")}］ <a href="{e(t["url"])}" style="color:{C["ink"]}">{e(t["title"])}</a></div>'
                for t in items
            )
        parts.append('<div style="font-size:12px;font-weight:bold;margin:6px 0 2px">閉じたもの</div>')
        parts.append(tlist([t for t in tickets if t["event"] != "created"], "この週に閉じたものはありません"))
        parts.append('<div style="font-size:12px;font-weight:bold;margin:10px 0 2px">新しく立ったもの</div>')
        parts.append(tlist([t for t in tickets if t["event"] != "closed"], "この週に立ったものはありません"))

    body = "".join(parts)
    return (
        '<!doctype html><html><body style="margin:0;padding:0;background:#ffffff">'
        f'<div style="max-width:640px;margin:0 auto;padding:16px;font-family:-apple-system,\'Hiragino Sans\','
        f'\'Noto Sans JP\',sans-serif;color:{C["ink"]}">{body}'
        f'<div style="font-size:10px;color:{C["muted"]};margin-top:24px">life_dashboard の週次フローから自動送信</div>'
        '</div></body></html>'
    )


def build_message(rep: dict, base_url: str, address: str) -> MIMEMultipart:
    charts = render_charts(rep)
    msg = MIMEMultipart("related")
    msg["Subject"] = f"週次レポート {_md(rep['weekStart'])}〜{_md(rep['weekEnd'])}"
    msg["From"] = address
    msg["To"] = address
    alt = MIMEMultipart("alternative")
    alt.attach(MIMEText(
        f"週次レポート {rep['weekStart']}〜{rep['weekEnd']}\n{base_url}/report?week_end={rep['weekEnd']}\n"
        "HTML 表示に対応したメールソフトで開いてください。", "plain", "utf-8"))
    alt.attach(MIMEText(render_html(rep, base_url, [cid for cid, _ in charts]), "html", "utf-8"))
    msg.attach(alt)
    for cid, png in charts:
        img = MIMEImage(png, "png")
        img.add_header("Content-ID", f"<{cid}>")
        img.add_header("Content-Disposition", "inline", filename=f"{cid}.png")
        msg.attach(img)
    return msg


# ─────────────────────────────────────────────────────────────
# 送信
# ─────────────────────────────────────────────────────────────

@task(name="Send weekly report mail", retries=2, retry_delay_seconds=60)
def send_weekly_report_mail(week_end: str | None = None) -> str:
    from prefect.blocks.system import Secret
    from prefect.variables import Variable

    address = Variable.get("report_mail_address", default=None)
    if not address:
        raise RuntimeError("Prefect Variable report_mail_address が未設定")
    # Google はアプリパスワードを「abcd efgh ijkl mnop」と4文字ずつ区切って表示する。
    # 区切りのスペースごと貼っても通るように取り除く（本体は16文字の英小文字）
    password = "".join(Secret.load("gmail-app-password").get().split())
    base_url = (Variable.get("life_dashboard_url", default=None) or DEFAULT_URL).rstrip("/")

    rep = fetch_report(base_url, week_end)
    msg = build_message(rep, base_url, address)
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=60) as s:
        s.login(address, password)
        s.send_message(msg)
    print(f"📧 週次レポートを送信: {rep['weekStart']}〜{rep['weekEnd']} → {address}")
    return rep["weekEnd"]


@flow(name="Weekly Report Mail", log_prints=True)
def weekly_report_mail_flow(week_end: str | None = None):
    """手動の再送用。週次フローの最後でも同じ task を呼んでいる。"""
    return send_weekly_report_mail(week_end)


if __name__ == "__main__":
    weekly_report_mail_flow()
