import logging
from typing import Dict, List, Any
from datetime import date, datetime, timedelta, timezone

from aw_client import ActivityWatchClient


# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class ActivityWatchScraper:
    def __init__(self, target_url: str = "http://aw.mynet"):
        self.client = ActivityWatchClient("prefect-extractor", testing=False)
        self.client.server_address = target_url

    def fetch_daily_data(self, target_date: date) -> Dict[str, List[Dict[str, Any]]]:
        """
        全バケットを動的に取得し、それぞれのバケットの生イベントを辞書形式で返す。
        戻り値: {"バケットID": [イベントリスト], "バケットID": [イベントリスト]}
        """
        # JSTの0:00〜23:59
        jst = timezone(timedelta(hours=9))
        start_dt = datetime(target_date.year, target_date.month, target_date.day, 0, 0, 0, tzinfo=jst)
        end_dt = start_dt + timedelta(days=1)
        logger.info(f"📅 取得対象期間(JST): {start_dt} 〜 {end_dt}")
        buckets = self.client.get_buckets()
        extracted_data = {}
        for bucket_id in buckets.keys():
            # client.get_events だと独自のEventオブジェクトに変換されてJSON保存時にエラーになるため、
            # 最も安全なAW Queryエンジンを使って生JSONを直接引っこ抜く
            query_str = f'RETURN = query_bucket("{bucket_id}");'
            try:
                result = self.client.query(query_str, [(start_dt, end_dt)])
                if result and result[0]:
                    extracted_data[bucket_id] = result[0]
                    logger.info(f"  -> 📦 {bucket_id}: {len(result[0])} 件取得")
            except Exception as e:
                logger.error(f"❌ {bucket_id} の取得エラー: {e}")
        return extracted_data


