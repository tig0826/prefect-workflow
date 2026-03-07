import pandas as pd
from time import sleep
from prefect import flow, task
from prefect import get_run_logger
import requests

from common.DQXPriceSearch import DQXPriceSearch
from common.trino_api import TrinoAPI
from common.DatetimeTranslator import DatetimeTranslator
from common.discord_notify import send_to_discord
from common.get_cheap_item_list import get_cheap_item_list, create_message_cheap_item
from common.get_hourly_price import create_message_kishoukaku_hourly_price
from common.get_price_graph import create_price_graph
from common.session_cookies import login_dqx_and_save_cookies
from common.exceptions import SessionExpiredException

trino_host = "trino.trino.svc.cluster.local"
trino_port = 8080
trino_user = "tig"
trino_catalog = "iceberg"


# 冒険者の広場から価格情報を取得するタスク
@task(
    name="get exhibit price",
    log_prints=True,
    # tags=["dqx_price"],
    timeout_seconds=60,
    retries=30,
    retry_delay_seconds=1,
)
def search_price(item_name, dt=None):
    try:
        dqx = DQXPriceSearch()
        df_price = dqx.search_price(item_name)
    except SessionExpiredException:
        print(
            f"🚨 [Self-Healing] Session expired detected for {item_name}. Refreshing cookies..."
        )
        # 1. 腐ったCookieを捨てて、Seleniumで新しいCookieを取得・保存する
        login_dqx_and_save_cookies()
        print("✅ New cookies obtained. Retrying the search instantly...")
        # 2. 新しいCookieがSecretに入った状態で、もう一度インスタンスを作り直す
        dqx = DQXPriceSearch()
        df_price = dqx.search_price(item_name)
    # 冒険者の広場から取得したデータには含まれていないカラムを追加
    if df_price is None:
        df_price = pd.DataFrame(
            columns=[
                "できのよさ",
                "個数",
                "価格",
                "1つあたりの価格",
                "出品開始",
                "出品終了",
                "出品者",
            ]
        )
    df_price["Name"] = item_name
    df_price["Date"] = dt.date() if dt else None
    df_price["Hour"] = dt.hour() if dt else None
    ts_str = dt.datetime(align_to="hour") if dt else None
    ts = pd.to_datetime(ts_str, format="%Y-%m-%d %H:%M:%S") if ts_str else pd.NaT
    df_price["observed_at"] = ts
    new_order = [
        "Name",
        "価格",
        "1つあたりの価格",
        "個数",
        "できのよさ",
        "出品開始",
        "出品終了",
        "出品者",
        "Date",
        "Hour",
        "observed_at",
    ]
    df_price = df_price[new_order]
    return df_price


@task(name="save table", retries=30, retry_delay_seconds=1)
def save_to_iceberg(table_name, schema_name, df):
    """自作のtrino APIを使用して、Icebergにデータを保存するタスク"""
    trino = TrinoAPI(
        host=trino_host, port=trino_port, user=trino_user, catalog=trino_catalog
    )
    trino.create_schema(schema_name)
    trino.create_table(
        table_name,
        schema_name,
        trino.extract_columns(df),
        partitioning=["day(observed_at)"],
    )
    trino.insert_table(table_name, schema_name, df)


@flow(log_prints=True)
def send_cheap_saibou_price():
    # discordからメッセージを受け取った際にやすい細胞の出品状況を通知する
    focus_item = ["魔因細胞", "閃魔細胞"]
    # 価格情報を取得するタスクを作成
    df_all_price = pd.DataFrame()
    for item_name in focus_item:
        df_price = search_price(item_name)
        df_all_price = pd.concat([df_all_price, df_price], ignore_index=True)
        sleep(0.3)
        item_name_kakera = item_name + "のかけら"
        df_price_kakera = search_price(item_name_kakera)
        df_all_price = pd.concat([df_all_price, df_price_kakera], ignore_index=True)
        sleep(0.3)
    # やすい出品情報を取得
    for item_name in focus_item:
        df_cheap = get_cheap_item_list(item_name, df_all_price)
        message = create_message_cheap_item(item_name, df_cheap)
        send_to_discord(message)


@flow(log_prints=True)
def get_price_hourly(skip_insert: bool = False):
    focus_item = [
        "輝晶核",
        "魔因細胞",
        "魔因細胞のかけら",
        "閃輝晶核",
        "閃魔細胞",
        "閃魔細胞のかけら",
    ]
    # 価格情報を取得する日時を取得
    dt = DatetimeTranslator()
    hour = dt.hour()
    weekday = dt.weekday()
    # 価格情報を取得するタスクを作成
    df_all_price = pd.DataFrame()
    schema_name = "dqx"
    table_name = "price_hourly"
    for item_name in focus_item:
        df_price = search_price(item_name, dt)
        # 価格情報を保存
        if len(df_price) > 0 and not skip_insert:
            save_to_iceberg(table_name, schema_name, df_price)
        df_all_price = pd.concat([df_all_price, df_price], ignore_index=True)
        sleep(0.7)
    # 価格情報からdiscordに送るメッセージを作成
    message = create_message_kishoukaku_hourly_price(df_all_price)
    # 輝晶核の価格を送信
    send_to_discord(message)
    focus_item_matrix = [
        [
            "輝晶核",
            "魔因細胞",
            "魔因細胞のかけら",
        ],
        ["閃輝晶核", "閃魔細胞", "閃魔細胞のかけら"],
    ]
    if hour == "21":
        # 21時の場合は１日分の価格推移のグラフを送信
        start_datetime = dt.datetime(days=-1, replace_hour=21)
        end_datetime = dt.datetime()
        for focus_item in focus_item_matrix:
            kaku_name = focus_item[0]
            temp_file_name = create_price_graph(
                focus_item,
                start_datetime,
                end_datetime,
                graph_title=f"{end_datetime} {kaku_name}の価格推移",
            )
            send_to_discord("", temp_file_name)
    if weekday == "Friday" and hour == "21":
        # 金曜日の21時の場合は週間の価格推移を送信
        start_datetime = dt.datetime(days=-7, replace_hour=21)
        end_datetime = dt.datetime()
        for focus_item in focus_item_matrix:
            kaku_name = focus_item[0]
            temp_file_name = create_price_graph(
                focus_item,
                start_datetime,
                end_datetime,
                graph_title=f"{start_datetime} ~ {end_datetime} {kaku_name}の価格推移",
            )
            send_to_discord("", temp_file_name)
    # martをアップデート
    # Prefect API のエンドポイント
    # PREFECT_API_URL = "http://prefect.mynet/api"
    PREFECT_API_URL = "http://prefect-server.prefect.svc.cluster.local:4200/api"
    PREFECT_DEPLOYMENT_ID_GET_PRICE_MART = "579ae2df-61a4-4dee-ab77-270e51323038"
    response = requests.post(
        f"{PREFECT_API_URL}/deployments/{PREFECT_DEPLOYMENT_ID_GET_PRICE_MART}/create_flow_run",
        json={},
    )
    # # supabaseのmrt_price_hourlyを更新するflowを起動
    # PREFECT_DEPLOYMENT_ID_SYNC_SUPABASE_MRT_PRICE_HOURLY = "b2b8b52b-ce84-426f-9ef8-884aed0873b7"
    # response = requests.post(
    #     f"{PREFECT_API_URL}/deployments/{PREFECT_DEPLOYMENT_ID_SYNC_SUPABASE_MRT_PRICE_HOURLY}/create_flow_run",
    #     json={}
    #     )


@flow(log_prints=True)
def send_price_image(period: str = "1w"):
    """
    discordから受け取ったメッセージを解析して、価格推移のグラフを生成する
    メッセージは!グラフ <期間>
    期間は◯d, ◯w, ◯m, のいずれか
    """
    logger = get_run_logger()
    print(f"受け取った period: {period}（type: {type(period)}）")
    dt = DatetimeTranslator()
    # 価格情報を取得する日時を取得
    # periodを数字と単位に分離
    period_num = int(period[:-1])
    period_unit = period[-1]
    print(f"period_num: {period_num}（type: {type(period_num)}）")
    # 該当区間のデータを画像として送信する
    if period_unit == "d":
        # 日単位
        start_datetime = dt.datetime(days=-period_num)
    elif period_unit == "w":
        # 週単位
        start_datetime = dt.datetime(weeks=-period_num)
    elif period_unit == "m":
        # 月単位
        start_datetime = dt.datetime(months=-period_num)
    else:
        raise ValueError("Invalid period unit. Use 'd', 'w', or 'm'.")
    end_datetime = dt.datetime()
    focus_item_matrix = [
        [
            "輝晶核",
            "魔因細胞",
            "魔因細胞のかけら",
        ],
        ["閃輝晶核", "閃魔細胞", "閃魔細胞のかけら"],
    ]
    for focus_item in focus_item_matrix:
        kaku_name = focus_item[0]
        try:
            logger.info(f"受け取った period: {period}（type: {type(period)}）")
            temp_file_name = create_price_graph(
                focus_item,
                start_datetime,
                end_datetime,
                graph_title=f"{start_datetime} ~ {end_datetime} {kaku_name}の価格推移",
            )
            send_to_discord("", temp_file_name)
        except Exception as e:
            logger.error(f"✨ send_price_image_to_discord で例外発生: {e}")


if __name__ == "__main__":
    # get_price.serve(name="dqx-get-price")
    # get_price_hourly.serve(name="dqx-get-price-hourly")
    send_price_image.serve(name="dqx-send-price-image")
