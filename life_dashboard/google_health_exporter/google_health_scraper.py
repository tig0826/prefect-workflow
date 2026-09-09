"""Google Health API から生データを取る。外部インフラ(Prefect等)に依存しない。

レガシー Fitbit Web API は 2026年9月に停止する。体重・体脂肪はそれより先に
配信が止まっていて（最後の実測は 2026-08-31）、アプリには値があるのに
レガシー API からは取れない状態だった。取得経路をここに移す。

**bronze には全ソース・全デバイスをそのまま保存する。**
同じ実測が最大3系統（GOOGLE_WEB_API / HEALTH_CONNECT / FITBIT_WEB_API）から
入り、歩数はさらに端末ごとに別レコードになる。どれを採用するかは silver 側の
判断なので、ここで捨てない。捨てると後から方針を変えるたびに再取り込みが必要に
なる（例: 時計の充電中の穴をスマホの歩数で埋めたくなった場合）。
"""

import json
import logging
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)

API_BASE = "https://health.googleapis.com/v4"
TOKEN_URL = "https://oauth2.googleapis.com/token"

JST = "+09:00"

# (パス, filter 式の member, レスポンス JSON のキー, 日付の取り出し方)
#
# 絞り込みに使える項目はデータ型ごとに違う。2026-09-09 に実測で確認した結果:
#   weight / body-fat … sample_time.physical_time
#   steps             … interval.start_time
#   sleep             … interval.end_time のみ。start_time は
#                       INVALID_DATA_POINT_FILTER_DATA_TYPE_MEMBER で拒否される
#
# 睡眠を「終了時刻の日付」に寄せるのは、レガシー Fitbit の dateOfSleep
# （起きた日に紐づく）と同じ意味にするため。フィルタに使える項目と一致するので
# 都合もいい。
DATA_TYPES = [
    ("weight", "weight.sample_time.physical_time", "weight", "sample"),
    ("body-fat", "body_fat.sample_time.physical_time", "bodyFat", "sample"),
    ("steps", "steps.interval.start_time", "steps", "interval_start"),
    ("sleep", "sleep.interval.end_time", "sleep", "interval_end"),
]


class GoogleHealthScraper:
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        token_dict: dict,
        refresh_cb: Optional[Callable[[dict], None]] = None,
    ):
        """
        Args:
            client_id / client_secret: OAuth クライアント
            token_dict: refresh_token を含むトークン辞書
            refresh_cb: Google が refresh_token を差し替えてきた場合に呼ぶ。
                        通常は差し替わらないが、黙って古いものを持ち続けると
                        次回以降 invalid_grant で落ちるので保存経路を用意する。
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_dict = dict(token_dict)
        self.refresh_cb = refresh_cb
        self._access_token: Optional[str] = None

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------
    @staticmethod
    def _http(
        url: str,
        *,
        method: str = "GET",
        headers: Optional[dict] = None,
        form_body: Optional[dict] = None,
    ) -> Any:
        data = urllib.parse.urlencode(form_body).encode() if form_body else None
        h = dict(headers or {})
        if data:
            h["Content-Type"] = "application/x-www-form-urlencoded"
        req = urllib.request.Request(url, data=data, headers=h, method=method)
        try:
            with urllib.request.urlopen(req, timeout=60) as res:
                raw = res.read()
                return json.loads(raw) if raw else None
        except urllib.error.HTTPError as e:
            body = e.read().decode(errors="replace")
            raise RuntimeError(f"{method} {url} -> {e.code}\n{body[:1000]}") from e

    def _refresh_access_token(self) -> str:
        logger.info("access_token を再発行する")
        res = self._http(
            TOKEN_URL,
            method="POST",
            form_body={
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "refresh_token": self.token_dict["refresh_token"],
                "grant_type": "refresh_token",
            },
        )
        self._access_token = res["access_token"]
        # Google は通常 refresh_token を返さない（既存のものが有効なまま）。
        # 返してきたときだけ保存する。
        if res.get("refresh_token") and res["refresh_token"] != self.token_dict.get(
            "refresh_token"
        ):
            self.token_dict["refresh_token"] = res["refresh_token"]
            if self.refresh_cb:
                self.refresh_cb(self.token_dict)
        return self._access_token

    def _get(self, path: str, query: dict) -> Any:
        if not self._access_token:
            self._refresh_access_token()
        url = f"{API_BASE}{path}?{urllib.parse.urlencode(query)}"
        try:
            return self._http(url, headers={"Authorization": f"Bearer {self._access_token}"})
        except RuntimeError as e:
            if " -> 401" not in str(e):
                raise
            self._refresh_access_token()
            return self._http(url, headers={"Authorization": f"Bearer {self._access_token}"})

    # ------------------------------------------------------------------
    # 取得
    # ------------------------------------------------------------------
    def _list_all(self, data_type: str, member: str, lo: str, hi: str) -> List[dict]:
        """nextPageToken を辿って全件返す。

        辿らないと件数を大幅に取りこぼす。実測では30日分の歩数が
        1ページ目だけだと42件、全ページ辿ると13,559件だった。
        """
        out: List[dict] = []
        token, pages = None, 0
        expr = f'{member} >= "{lo}" AND {member} < "{hi}"'
        while True:
            query = {"filter": expr, "pageSize": 1000}
            if token:
                query["pageToken"] = token
            res = self._get(f"/users/me/dataTypes/{data_type}/dataPoints", query)
            out.extend((res or {}).get("dataPoints") or [])
            token = (res or {}).get("nextPageToken")
            pages += 1
            if not token:
                break
            if pages >= 500:
                logger.warning(f"{data_type}: 500ページで打ち切った。取りこぼしの可能性あり")
                break
        logger.info(f"{data_type}: {len(out)}件 / {pages}ページ")
        return out

    @staticmethod
    def _civil_date(point: dict, body_key: str, mode: str) -> Optional[str]:
        """そのレコードが属する JST の暦日を返す。

        UTC で切ると日本時間の夜が翌日に流れるため、API が返す civilTime
        （端末のローカル時刻 = JST）を使う。
        """
        body = point.get(body_key) or {}
        if mode == "sample":
            d = ((body.get("sampleTime") or {}).get("civilTime") or {}).get("date")
        else:
            iv = body.get("interval") or {}
            key = "civilStartTime" if mode == "interval_start" else "civilEndTime"
            d = (iv.get(key) or {}).get("date")
            if not d:
                # sleep は civilEndTime を持たない場合がある。
                # UTC の時刻とオフセットから自力で JST に直す。
                raw = iv.get("endTime" if mode == "interval_end" else "startTime")
                off = iv.get("endUtcOffset" if mode == "interval_end" else "startUtcOffset")
                if not raw:
                    return None
                try:
                    ts = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                    secs = int(str(off).rstrip("s")) if off else 9 * 3600
                    return (ts + timedelta(seconds=secs)).strftime("%Y-%m-%d")
                except (ValueError, TypeError):
                    return None
        if not d:
            return None
        return f"{d['year']:04d}-{d['month']:02d}-{d['day']:02d}"

    def fetch_range(self, start: date, end: date) -> Dict[str, Dict[str, list]]:
        """start〜end（両端含む・JST日付）のデータを日付ごとにまとめて返す。

        日付ごとに個別のクエリを投げるのではなく、期間を一度に引いてから
        JST の暦日で振り分ける。API 呼び出しが日数分の1になる。

        Returns: {"2026-09-09": {"weight": [...], "steps": [...], ...}, ...}
        """
        # JST の [start 00:00, end+1 00:00) を UTC の境界に直す
        lo = f"{start.isoformat()}T00:00:00{JST}"
        hi = f"{(end + timedelta(days=1)).isoformat()}T00:00:00{JST}"
        logger.info(f"取得範囲 (JST): {lo} 〜 {hi}")

        buckets: Dict[str, Dict[str, list]] = {}
        cursor = start
        while cursor <= end:
            buckets[cursor.isoformat()] = {k: [] for k, _, _, _ in DATA_TYPES}
            cursor += timedelta(days=1)

        for data_type, member, body_key, mode in DATA_TYPES:
            try:
                points = self._list_all(data_type, member, lo, hi)
            except Exception as e:
                # 1つのデータ型が落ちても他を止めない。ただし黙って
                # 空を保存すると正常なパーティションを潰すので、
                # 呼び出し側が欠けを検知できるよう None を残す。
                logger.error(f"{data_type} の取得に失敗した (Skipping): {e}")
                for day in buckets:
                    buckets[day][data_type] = None
                continue

            dropped = 0
            for p in points:
                day = self._civil_date(p, body_key, mode)
                if day is None:
                    dropped += 1
                    continue
                if day in buckets:
                    buckets[day][data_type].append(p)
            if dropped:
                logger.warning(f"{data_type}: 日付を決められず捨てた {dropped}件")

        return buckets
