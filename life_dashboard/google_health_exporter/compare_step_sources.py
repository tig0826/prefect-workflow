"""歩数・睡眠をどの platform から取るべきかを実測で決めるための調査スクリプト。

Google Health には同じ実測が複数の platform から入る。歩数は1分刻みの
区間データなので、素朴に全部足すと2〜3倍に膨らむ。どれか1つを選ぶ必要が
あるが、「点数が多い方が網羅的」とは限らない（スマホと時計の二重計上で
増えている可能性がある）ので、日次合計を既存の gold テーブルの値と
突き合わせて決める。

既存 gold (mrt_fitness_daily_summary) はレガシー Fitbit Web API 由来で、
ダッシュボードが数年分表示してきた数値そのもの。これに一致する platform を
選べば、移行しても推移グラフに段差ができない。

Prefect Secret を読み取り専用で参照する。API も GET だけで、
トークンのリフレッシュは access_token の再発行のみ（refresh_token は変わらない）。

使い方:
    uv run python -m google_health_exporter.compare_step_sources
"""

import json
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime

PREFECT_API = "http://prefect.mynet/api"
HEALTH_API_BASE = "https://health.googleapis.com/v4"

# 既存 gold の値（レガシー Fitbit Web API 由来 / JST日付）。
# これがダッシュボードの表示履歴なので、比較の基準にする。
GOLD_STEPS = {
    "2026-09-01": 2307,
    "2026-09-02": 3159,
    "2026-09-03": 7656,
    "2026-09-04": 11767,
    "2026-09-05": 9860,
    "2026-09-06": 11995,
    "2026-09-07": 907,
    "2026-09-08": 1040,
}


def _request(url, method="GET", headers=None, json_body=None, form_body=None):
    data, h = None, dict(headers or {})
    if json_body is not None:
        data = json.dumps(json_body).encode()
        h["Content-Type"] = "application/json"
    elif form_body is not None:
        data = urllib.parse.urlencode(form_body).encode()
        h["Content-Type"] = "application/x-www-form-urlencoded"
    req = urllib.request.Request(url, data=data, headers=h, method=method)
    try:
        with urllib.request.urlopen(req) as res:
            raw = res.read()
            return json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"{method} {url} -> {e.code}\n{e.read().decode(errors='replace')[:600]}") from e


def _secret(name):
    docs = _request(
        f"{PREFECT_API}/block_documents/filter",
        method="POST",
        json_body={"block_documents": {"name": {"any_": [name]}}},
    )
    if not docs:
        raise SystemExit(f"Prefect Secret '{name}' が無い")
    return _request(f"{PREFECT_API}/block_documents/{docs[0]['id']}?include_secrets=true")["data"]["value"]


def _access_token():
    tok = json.loads(_secret("google-health-token"))
    return _request(
        "https://oauth2.googleapis.com/token",
        method="POST",
        form_body={
            "client_id": _secret("google-health-client-id"),
            "client_secret": _secret("google-health-client-secret"),
            "refresh_token": tok["refresh_token"],
            "grant_type": "refresh_token",
        },
    )["access_token"]


def _list_all(data_type, member, lo, hi, headers):
    """nextPageToken を辿って全件返す。辿らないと件数を大幅に過小評価する。"""
    out, token, pages = [], None, 0
    expr = f'{member} >= "{lo}" AND {member} < "{hi}"'
    while True:
        query = {"filter": expr, "pageSize": 1000}
        if token:
            query["pageToken"] = token
        res = _request(
            f"{HEALTH_API_BASE}/users/me/dataTypes/{data_type}/dataPoints?" + urllib.parse.urlencode(query),
            headers=headers,
        )
        out.extend((res or {}).get("dataPoints") or [])
        token = (res or {}).get("nextPageToken")
        pages += 1
        if not token or pages >= 200:
            break
    return out, pages


def _civil_date(interval):
    """JST の暦日を取り出す。UTC で切ると日本時間の夜が翌日に流れる。"""
    d = (interval.get("civilStartTime") or {}).get("date") or {}
    if not d:
        return None
    return f"{d['year']:04d}-{d['month']:02d}-{d['day']:02d}"


def main():
    headers = {"Authorization": f"Bearer {_access_token()}"}
    lo, hi = "2026-08-31T15:00:00Z", "2026-09-09T15:00:00Z"  # JST 9/1 00:00 〜 9/10 00:00

    print("=== 歩数: platform 別の日次合計 vs 既存 gold ===")
    points, pages = _list_all("steps", "steps.interval.start_time", lo, hi, headers)
    print(f"取得 {len(points)} 件 / {pages} ページ\n")

    # platform だけで束ねると、同じ platform 内で複数デバイスが同じ歩数を
    # 数えている二重計上が見えない。実測では FITBIT の中に MobileTrack
    # （スマホの歩数計）と Pixel Watch 3 の両方が入っていて、合計が
    # 既存 gold のほぼ2倍になっていた。デバイス単位まで割る。
    per = defaultdict(lambda: defaultdict(int))          # "platform/device" -> date -> steps
    intervals = defaultdict(lambda: defaultdict(list))
    for p in points:
        src = p.get("dataSource") or {}
        plat = src.get("platform") or "?"
        dev = (src.get("device") or {}).get("displayName")
        app = (src.get("application") or {}).get("googleWebClientId") or (src.get("application") or {}).get("webClientId")
        key = f"{plat}/{dev or app or '不明'}"
        iv = p["steps"]["interval"]
        date = _civil_date(iv)
        if not date:
            continue
        per[key][date] += int(p["steps"]["count"])
        intervals[key][date].append((iv["startTime"], iv["endTime"]))

    keys = sorted(per.keys())
    w = max(len(k) for k in keys) + 3
    print("日付        " + "".join(f"{k:>{w}}" for k in keys) + f"{'gold(既存)':>14}")
    print("-" * (12 + w * len(keys) + 14))
    for date in sorted(GOLD_STEPS):
        row = f"{date}  "
        for k in keys:
            row += f"{per[k].get(date, 0):>{w},}"
        row += f"{GOLD_STEPS[date]:>14,}"
        print(row)

    print("\n--- gold との一致度（比が 1.00 に近いものが既存の数値と同じ出どころ）---")
    for k in keys:
        rel = [abs(per[k].get(d, 0) - g) / g for d, g in GOLD_STEPS.items() if g]
        ratio = [per[k].get(d, 0) / g for d, g in GOLD_STEPS.items() if g]
        print(
            f"  {k:<{w}} 平均相対差 {100*sum(rel)/len(rel):>6.1f}%"
            f"   gold比 平均 {sum(ratio)/len(ratio):>5.2f}倍"
        )

    print("\n--- 区間の重なり（そのデバイス単体で二重計上していないか）---")
    for k in keys:
        overlap_days = 0
        for date, ivs in intervals[k].items():
            s = sorted(ivs)
            if any(s[i][1] > s[i + 1][0] for i in range(len(s) - 1)):
                overlap_days += 1
        print(f"  {k:<{w}} 重なりのある日: {overlap_days} / {len(intervals[k])} 日")

    print("\n\n=== 睡眠: platform 別 ===")
    sleeps, _ = _list_all("sleep", "sleep.interval.end_time", lo, hi, headers)
    by_plat = defaultdict(list)
    for p in sleeps:
        by_plat[(p.get("dataSource") or {}).get("platform") or "?"].append(p)
    for plat, ps in sorted(by_plat.items()):
        staged = sum(1 for p in ps if p["sleep"].get("type") == "STAGES")
        # 睡眠段階のうち LIGHT/DEEP/REM の実長を足す。AWAKE は除く
        # （レガシー v1 が restless を含めて約20%多く出ていた件と同じ扱い）。
        asleep_min = 0
        for p in ps:
            for st in p["sleep"].get("stages") or []:
                if st.get("type") in ("LIGHT", "DEEP", "REM"):
                    a = datetime.fromisoformat(st["startTime"].replace("Z", "+00:00"))
                    b = datetime.fromisoformat(st["endTime"].replace("Z", "+00:00"))
                    asleep_min += (b - a).total_seconds() / 60
        print(
            f"  {plat:<18} セッション {len(ps):>3} 件 / STAGES 付き {staged} 件"
            f" / 睡眠実時間 合計 {asleep_min/60:.1f}h"
        )
        for p in ps[:2]:
            iv = p["sleep"]["interval"]
            print(f"      {iv['startTime']} -> {iv['endTime']}  stages={len(p['sleep'].get('stages') or [])}")


if __name__ == "__main__":
    main()
