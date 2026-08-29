import json
import logging
import datetime
from typing import Dict, Any, Optional, Callable

import fitbit

logger = logging.getLogger(__name__)


class FitbitScraper:
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        token_dict: dict,
        refresh_cb: Callable[[dict], None],
    ):
        """
        純粋なFitbitスクレイパークラス。外部のインフラ(Prefect等)に依存しない。

        Args:
            client_id: Fitbit APIのクライアントID
            client_secret: Fitbit APIのシークレット
            token_dict: 現在のトークン情報 (access_token, refresh_token等を含む辞書)
            refresh_cb: トークンが更新された際に呼び出されるコールバック関数
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_dict = token_dict
        self.refresh_cb = refresh_cb
        # クライアント初期化
        self.client = self._initialize_client()

    def _initialize_client(self) -> fitbit.Fitbit:
        """Fitbitクライアントを初期化する"""
        try:
            return fitbit.Fitbit(
                self.client_id,
                self.client_secret,
                access_token=self.token_dict.get("access_token"),
                refresh_token=self.token_dict.get("refresh_token"),
                expires_at=self.token_dict.get("expires_at"),
                refresh_cb=self.refresh_cb,  # 外から注入されたコールバックを渡す
            )
        except TypeError:
            logger.warning("古いFitbitライブラリ仕様でフォールバック初期化します。")
            return fitbit.Fitbit(
                self.client_id,
                self.client_secret,
                access_token=self.token_dict.get("access_token"),
                refresh_token=self.token_dict.get("refresh_token"),
                refresh_cb=self.refresh_cb,
            )

    def _safe_fetch(self, name: str, fetch_func) -> Optional[Any]:
        """APIコールを安全に実行し、失敗しても後続を止めないラッパー"""
        logger.info(f"... {name} データ取得中")
        try:
            return fetch_func()
        except Exception as e:
            logger.error(f"{name} の取得に失敗した (Skipping): {e}")
            return None

    def _get(self, path: str) -> Optional[Any]:
        """python-fitbit が用意していないエンドポイントを直接叩く。

        make_request はトークンのリフレッシュとエラー処理をライブラリ側でやるので、
        自前で requests を書くより安全。
        """
        return self.client.make_request(f"{self.client.API_ENDPOINT}{path}", method="GET")

    def fetch_daily_data(self, target_date: datetime.date) -> Dict[str, Any]:
        date_str = target_date.strftime("%Y-%m-%d")
        logger.info(f"{date_str} のFitbitデータを取得開始...")
        activities = self._safe_fetch(
            "アクティビティ", lambda: self.client.activities(date=target_date)
        )
        # v1（classic）。既存の silver / gold が参照しているので互換のため残す。
        sleep = self._safe_fetch("睡眠", lambda: self.client.sleep(date=target_date))
        # v1.2（stages）。python-fitbit の client.sleep() は v1 を叩くため別途取得する。
        #
        # v1 との違い（2026-08-27 実測）:
        #   ・v1 は restless を睡眠に含めるため合計が約20%多い
        #     （8/26 の3セッション合計で v1=524分 / v1.2=431分）
        #   ・v1.2 だけが levels.data（睡眠段階の区間時系列）、levels.shortData（短時間覚醒）、
        #     levels.summary の count（深睡眠4回 / 覚醒13回など）、
        #     thirtyDayAvgMinutes（Fitbit 自身の30日平均）を持つ
        #   ・覚醒区間の時刻が分かるので、解錠タイムスタンプや画面時間と照合できる
        sleep_stages = self._safe_fetch(
            "睡眠(v1.2 stages)",
            lambda: self._get(f"/1.2/user/-/sleep/date/{date_str}.json"),
        )
        body = self._safe_fetch("身体", lambda: self.client.body(date=target_date))
        heart_raw = self._safe_fetch(
            "心拍数(1分刻み)",
            lambda: self.client.intraday_time_series(
                resource="activities/heart", base_date=target_date, detail_level="1min"
            ),
        )
        # 以下は python-fitbit に対応メソッドが無いので直接叩く。
        # いずれも睡眠中の計測なので欠測が多い（実測カバレッジ 55%前後）。
        # 欠測日は「値が無い」だけで「良い」ではないため、下流で 0 埋めしないこと。
        hrv = self._safe_fetch("HRV", lambda: self._get(f"/1/user/-/hrv/date/{date_str}.json"))
        br = self._safe_fetch("呼吸数", lambda: self._get(f"/1/user/-/br/date/{date_str}.json"))
        spo2 = self._safe_fetch("SpO2", lambda: self._get(f"/1/user/-/spo2/date/{date_str}.json"))
        skin_temp = self._safe_fetch(
            "皮膚温", lambda: self._get(f"/1/user/-/temp/skin/date/{date_str}.json")
        )
        cardio = self._safe_fetch(
            "心肺フィットネス", lambda: self._get(f"/1/user/-/cardioscore/date/{date_str}.json")
        )
        azm = self._safe_fetch(
            "AZM",
            lambda: self._get(
                f"/1/user/-/activities/active-zone-minutes/date/{date_str}/1d.json"
            ),
        )
        raw_dict = {
            "date": date_str,
            "activities": activities,
            "sleep": sleep,
            "sleep_stages": sleep_stages,
            "body": body,
            "heart": heart_raw,
            "hrv": hrv,
            "br": br,
            "spo2": spo2,
            "skin_temp": skin_temp,
            "cardio": cardio,
            "azm": azm,
        }
        return {"raw_json": json.dumps(raw_dict), "dt": date_str}
