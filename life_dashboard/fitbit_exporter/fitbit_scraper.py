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

    def fetch_daily_data(self, target_date: datetime.date) -> Dict[str, Any]:
        date_str = target_date.strftime("%Y-%m-%d")
        logger.info(f"{date_str} のFitbitデータを取得開始...")
        activities = self._safe_fetch(
            "アクティビティ", lambda: self.client.activities(date=target_date)
        )
        sleep = self._safe_fetch("睡眠", lambda: self.client.sleep(date=target_date))
        body = self._safe_fetch("身体", lambda: self.client.body(date=target_date))
        heart_raw = self._safe_fetch(
            "心拍数(1分刻み)",
            lambda: self.client.intraday_time_series(
                resource="activities/heart", base_date=target_date, detail_level="1min"
            ),
        )
        raw_dict = {
            "date": date_str,
            "activities": activities,
            "sleep": sleep,
            "body": body,
            "heart": heart_raw,
        }
        return {"raw_json": json.dumps(raw_dict), "dt": date_str}
