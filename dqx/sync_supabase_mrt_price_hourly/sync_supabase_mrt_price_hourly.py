import pandas as pd
from prefect import flow, task
from prefect import get_run_logger

from supabase import create_client, Client
from prefect.blocks.system import Secret

from common.trino_api import TrinoAPI

trino_host = 'trino.trino.svc.cluster.local'
trino_port = 8080
trino_user = 'tig'
trino_catalog = 'iceberg'



class SupabaseDB:
    def __init__(self):
        url = Secret.load("supabase-url").get()
        key = Secret.load("supabase-key").get()
        self.client: Client = create_client(url, key)
        self.database_name = "dqx_trade_db"
    def add_record(self, record):
        """取引記録を追加"""
        try:
            self.client.table("mrt_price_hourly").upsert(record).execute()
            return True
        except Exception as e:
            print(f"レコード追加失敗: {e}")
            return False

@task(retries=3, retry_delay_seconds=10)
def get_price_data():
    logger = get_run_logger()
    trino = TrinoAPI(host=trino_host,
                     port=trino_port,
                     user=trino_user,
                     catalog=trino_catalog)
    query = """
            SELECT *
            FROM dqx.mrt_price_hourly
            WHERE ts_hour = (
              SELECT max(ts_hour)
              FROM dqx.mrt_price_hourly
            )
            """
    df = trino.execute_query(query)
    logger.info(f"Fetched {len(df)} records from Trino")
    focus_columns = ['item_id',
                     'ts_hour',
                     'p5_price']
    df = df[focus_columns]
    # 価格を小数点第一位で丸める
    df.loc[:, "p5_price"] = df["p5_price"].round(1)
    # supabaseに登録するためにデータ型を変換
    df["ts_hour"] = df["ts_hour"].astype(str)
    return df

@task(retries=3, retry_delay_seconds=10)
def add_price_record(record):
    supabase_db = SupabaseDB()
    logger = get_run_logger()
    success = supabase_db.add_record(record)
    if success:
        logger.info(f"Record added: {record}")
    else:
        logger.error(f"Failed to add record: {record}")


@flow(log_prints=True)
def sync_supabase_mrt_price_hourly():
    logger = get_run_logger()
    df = get_price_data()
    record = df.to_dict(orient='records')
    add_price_record(record)
