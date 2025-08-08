from plotly.subplots import make_subplots
import plotly.graph_objects as go
import tempfile
import pandas as pd
from time import sleep
from datetime import datetime, timedelta, timezone
import pytz
from prefect import flow, task
from prefect import get_run_logger

from common.DQXPriceSearch import DQXPriceSearch
from common.trino_api import TrinoAPI
from common.AnalyzePrice import AnalyzePrice
from common.discord_notify import send_to_discord


# 冒険者の広場から価格情報を取得するタスク
@task(name="get exhibit price",
      log_prints=True,
      tags=["dqx", "dqx_price"],
      timeout_seconds=60,
      retries=30,
      retry_delay_seconds=1)
def search_price(item_name):
    dqx = DQXPriceSearch()
    df_price = dqx.search_price(item_name)
    print(df_price)
    return df_price


# 価格情報を保存するためのデータフレームに変換
# 冒険者の広場から取得したデータには含まれていないカラムを追加
def convert_to_iceberg_format(df, item_name, today, hour):
    df['Name'] = item_name
    df['Date'] = today
    df['Hour'] = hour
    return df


@task(name="save table",
      retries=30,
      retry_delay_seconds=1)
def save_to_iceberg(table_name, schema_name, df):
    u"""自作のtrino APIを使用して、Icebergにデータを保存するタスク"""
    trino = TrinoAPI(host='trino.mynet',
                     port=80,
                     user='tig',
                     catalog='iceberg')
    trino.create_schema(schema_name)
    trino.create_table(table_name,
                       schema_name,
                       trino.extract_columns(df),
                       partitioning=["Name", "Date", "Hour"])
    trino.insert_table(table_name, schema_name, df)


@flow(log_prints=True)
def get_price():
    # 価格情報を取得する日時を取得
    utc_now = datetime.now(timezone.utc)
    jst = pytz.timezone('Asia/Tokyo')
    jst_now = utc_now.astimezone(jst)
    today = jst_now.strftime("%Y-%m-%d")  # JSTの日付
    hour = jst_now.strftime("%H")
    # 価格情報を取得するアイテムのリストを取得
    trino = TrinoAPI(host='trino.mynet',
                     port=80,
                     user='tig',
                     catalog='iceberg')
    df_metadata_weapon = trino.load('metadata_weapon', 'dqx')
    df_metadata_armor = trino.load('metadata_armor', 'dqx')
    df_metadata_dougu = trino.load('metadata_dougu', 'dqx')
    df_metadata = pd.concat([df_metadata_weapon,
                             df_metadata_armor,
                             df_metadata_dougu],
                            ignore_index=True)
    # 価格情報を取得するタスクを作成
    for i in range(len(df_metadata)):
        df = df_metadata.iloc[i]
        item_name = df['アイテム名']
        df_price = search_price(item_name)
        df_price = convert_to_iceberg_format(df_price, item_name, today, hour)
        # 価格情報を保存
        schema_name = "dqx"
        table_name = "price"
        # カラムを並び替える順序をリストで指定
        new_order = ["Name", "価格", "1つあたりの価格", "個数", "できのよさ",
                     "出品開始", "出品終了", "出品者", "Date", "Hour"]
        df_price = df_price[new_order]
        if len(df_price) > 0:
            save_to_iceberg(table_name, schema_name, df_price)
        sleep(0.7)


@task(name="get cheap saibou to send to discord",
      retries=5,
      retry_delay_seconds=1)
def send_cheap_saibou_price_to_discord(df_all):
    # やすい出品情報を取得
    focus_item = [["魔因細胞", "魔因細胞のかけら"],
                  ["閃魔細胞", "閃魔細胞のかけら"]]
    message = ["🛒 **お手頃な細胞・かけらの出品状況**",
               "※ 冒険者の広場側でのデータ公開やdiscordへの通知までの遅延があります",
               "※ 参考程度に御覧ください...",
               "※ 欠片は一定個数以下の出品を除外しています。"]
    percentile = 0.05  # 参考価格のパーセンタイル。これよりも大きく安いものを取得
    quantity_threshold = 10  # 一定個数以下のかけらの価格を排除。安く出品されていても買わないため
    discount_margin = 500  # 参考価格よりも安いものを取得するためのマージン
    top_n = 15  # 安い順に取得する件数
    for item_list in focus_item:
        df = df_all[df_all["Name"].isin(item_list)].copy()
        name_saibou = item_list[0]
        name_kakera = item_list[1]
        message.append("-------------------------------")
        message.append(f"**{name_saibou}の出品情報**")
        df_saibou = df[df["Name"] == name_saibou].copy()
        df_kakera = df[df["Name"] == name_kakera].copy()
        # 欠片はしきい値よりも出品数が小さいものを除外
        df_kakera = df_kakera[df_kakera["個数"] > quantity_threshold]
        # 細胞単位の価格に変換
        df_saibou["細胞価格"] = df_saibou["1つあたりの価格"]
        df_kakera["細胞価格"] = df_kakera["1つあたりの価格"]*20
        # 基準価格
        five_percent_price_saibou = df_saibou["細胞価格"].quantile(percentile)
        five_percent_price_kakera = df_kakera["細胞価格"].quantile(percentile)
        if five_percent_price_saibou <= five_percent_price_kakera:
            # 細胞のほうが欠片より安い場合
            base_price = five_percent_price_saibou
            message.append(f"現在の価格の目安: {name_saibou} **{int(base_price):,}G**")
        else:
            base_price = five_percent_price_kakera
            message.append(f"現在の価格の目安: {name_kakera} **{int(base_price/20):,}G**")
            message.append(f"※ 細胞1個に換算した価格は**{int(base_price):,}G**")
        # 欠片と細胞のdfを再度結合
        df_append = pd.concat([df_saibou, df_kakera], ignore_index=True)
        # 安い順にソート
        df_append = df_append.sort_values(by="1つあたりの価格", ascending=True)
        # しきい値よりも安いものを取得
        df_cheap = df_append[df_append["細胞価格"] <= base_price-discount_margin]
        if len(df_cheap) < 3:
            # もし５件以下の場合は安い順に3件選ぶ
            df_cheap = df_append.head(3)
        else:
            # 安い順に10まで件選ぶ
            df_cheap = df_cheap.head(top_n)
        # メッセージを作成
        for _, row in df_cheap.iterrows():
            item_name = row["Name"]
            unit_price = row["1つあたりの価格"]
            quantity = row["個数"]
            seller = row["出品者"]
            message.append(f"{item_name} : 単価**{unit_price:,}G** {quantity}個 出品者: {seller}")
    message = "\n".join(message)
    send_to_discord(message)


@task(name="send price message to discord",
      retries=5,
      retry_delay_seconds=1)
def create_price_message_to_discord(df, focus_item, jst_now):
    message = ""
    percentile = 0.05
    quantity_threshold = 3  # 一定個数以上の核の価格を排除。まとめ売りを排除するため
    for item_name in focus_item:
        if item_name in ["輝晶核", "閃輝晶核"]:
            # 核の場合は安く大量出品している人がいるので除外
            five_percent_price = df[(df["Name"] == item_name) & (df["個数"] <= quantity_threshold)]["1つあたりの価格"].quantile(percentile)
        else:
            five_percent_price = df[df["Name"] == item_name]["1つあたりの価格"].quantile(percentile)
        message_price = f"**{item_name}** : " + "{:,}".format(int(five_percent_price))+"G\n"
        message += message_price
    # 利益の期待値を計算
    # 細胞1個かかけら20個の内安い方で計算
    if "魔因細胞" in focus_item:
        price_mainsaibou = min(df[df["Name"] == "魔因細胞"]["1つあたりの価格"].quantile(percentile),
                               df[df["Name"] == "魔因細胞のかけら"]["1つあたりの価格"].quantile(percentile)*20)
        # Nameが輝晶核で列名"個数"の値がquantity_threshold以上の核のデータを排除
        price_kishoukaku = df[
                (df["Name"] == "輝晶核") & (df["個数"] <= quantity_threshold)
                ]["1つあたりの価格"].quantile(percentile)
    elif "閃魔細胞" in focus_item:
        price_mainsaibou = min(df[df["Name"] == "閃魔細胞"]["1つあたりの価格"].quantile(percentile),
                               df[df["Name"] == "閃魔細胞のかけら"]["1つあたりの価格"].quantile(percentile)*20)
        price_kishoukaku = df[
                (df["Name"] == "閃輝晶核") & (df["個数"] <= quantity_threshold)
                ]["1つあたりの価格"].quantile(percentile)
    profit = price_kishoukaku*(1*0.1+(75/99)*0.5+(45/99)*0.4)*0.95*4 - price_mainsaibou*30
    profit = int(profit)
    message_profit = "**一周持寄の利益期待値**: "+"{:,}".format(profit)+"G\n"
    message += message_profit
    return message


@task(name="send price image to discord",
      retries=5,
      retry_delay_seconds=1)
def send_price_image_to_discord(focus_item, start_datetime, end_datetime, graph_title="価格推移"):
    percentile = 0.05
    quantity_threshold = 3  # 一定個数以上の核の価格を排除。まとめ売りを排除するため
    # 閃輝晶核の価格推移を表示
    fig = make_subplots(
        rows=1,
        cols=len(focus_item),
        subplot_titles=focus_item
    )
    for idx, item_name in enumerate(focus_item, start=1):
        datetime_range = {"start": start_datetime, "end": end_datetime}
        analyze = AnalyzePrice("price_hourly", datetime_range)
        # 一日の間の価格推移を取得
        if item_name in ["輝晶核", "閃輝晶核"]:
            # 核の場合は安く大量出品している人がいるので除外
            df_price = analyze.percentile_price_by_hour(item_name,
                                                        percentile,
                                                        quantity_threshold=quantity_threshold)
        else:
            df_price = analyze.percentile_price_by_hour(item_name, percentile)
        fig.add_trace(
            go.Scatter(x=df_price.index, y=df_price, mode='lines+markers', name=item_name),
            row=1,
            col=idx
        )
    fig.update_layout(
        title=graph_title,
        xaxis_title="時刻",
        yaxis_title="価格",
        font=dict(family="Yu Gothic, MS Gothic, Meiryo, Noto Sans CJK JP, Arial, sans-serif", size=14),
        template='plotly_dark',
        showlegend=False  # 各サブプロットで個別に凡例を表示する場合は True に
    )
    # 一時ファイルに保存
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".png")
    fig.write_image(temp_file.name)
    send_to_discord("", temp_file.name)



@flow(log_prints=True)
def get_price_hourly(skip_insert: bool = False):
    focus_item = ["輝晶核", "魔因細胞", "魔因細胞のかけら",
                  "閃輝晶核", "閃魔細胞", "閃魔細胞のかけら"]
    # 価格情報を取得する日時を取得
    utc_now = datetime.now(timezone.utc)
    jst = pytz.timezone('Asia/Tokyo')
    jst_now = utc_now.astimezone(jst)
    today = jst_now.strftime("%Y-%m-%d")  # JSTの日付
    hour = jst_now.strftime("%H")
    weekday = jst_now.strftime("%A")
    last_week = (jst_now - timedelta(days=7)).strftime("%Y-%m-%d")
    # 価格情報を取得する対象のアイテム名などのメタデータを取得
    trino = TrinoAPI(host='trino.mynet',
                     port=80,
                     user='tig',
                     catalog='iceberg')
    df_metadata_weapon = trino.load('metadata_weapon', 'dqx')
    df_metadata_armor = trino.load('metadata_armor', 'dqx')
    df_metadata_dougu = trino.load('metadata_dougu', 'dqx')
    df_metadata = pd.concat([df_metadata_weapon,
                             df_metadata_armor,
                             df_metadata_dougu],
                            ignore_index=True)
    # 価格情報を取得するタスクを作成
    df_all_price = pd.DataFrame()
    for item_name in focus_item:
        df_price = search_price(item_name)
        df_price = convert_to_iceberg_format(df_price,
                                             item_name,
                                             today,
                                             hour)
        # 価格情報を保存
        schema_name = "dqx"
        table_name = "price_hourly"
        # カラムを並び替える順序をリストで指定
        new_order = ["Name", "価格", "1つあたりの価格", "個数", "できのよさ",
                     "出品開始", "出品終了", "出品者", "Date", "Hour"]
        df_price = df_price[new_order]
        df_all_price = pd.concat([df_all_price, df_price],
                                 ignore_index=True)
        if len(df_price) > 0 and not skip_insert:
            save_to_iceberg(table_name, schema_name, df_price)
        sleep(0.7)
    # discordに通知
    # 輝晶核の価格を送信
    if not skip_insert:
        datetime_now = today + " " + hour + ":00:00"
    else:
        # 現在の時刻を取得
        datetime_now = jst_now.strftime("%Y-%m-%d %H:%M:%S")
    message = f"**{datetime_now} の価格情報**\n"
    message += "----------------------------------\n"
    forcus_item_kishoukaku = ["輝晶核", "魔因細胞", "魔因細胞のかけら"]
    message += create_price_message_to_discord(df_all_price, forcus_item_kishoukaku, jst_now)
    message += "----------------------------------\n"
    # 閃輝晶核の価格を送信
    focus_item_senkishoukaku = ["閃輝晶核", "閃魔細胞", "閃魔細胞のかけら"]
    message += create_price_message_to_discord(df_all_price, focus_item_senkishoukaku, jst_now)
    message += "----------------------------------\n"
    send_to_discord(message)
    if hour == "21":
        # 21時の場合は１日分の価格推移のグラフを送信
        yesterday_21 = (jst_now - timedelta(days=1)).replace(hour=21, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
        today_21 = jst_now.replace(hour=21, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
        # 輝晶核の価格推移を表示
        send_price_image_to_discord(forcus_item_kishoukaku,
                                    yesterday_21,
                                    today_21,
                                    graph_title=f"{today} 輝晶核価格推移")
        # 閃輝晶核の価格推移を表示
        send_price_image_to_discord(focus_item_senkishoukaku,
                                    yesterday_21,
                                    today_21,
                                    graph_title=f"{today} 閃輝晶核価格推移")
    if weekday == "Friday" and hour == "21":
        # 金曜日の21時の場合は週間の価格推移を送信
        lastweek_21 = (jst_now - timedelta(days=7)).replace(hour=21, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
        today_21 = jst_now.replace(hour=21, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
        send_price_image_to_discord(forcus_item_kishoukaku,
                                    lastweek_21,
                                    today_21,
                                    graph_title=f"{last_week} ~ {today} 輝晶核価格推移")
        send_price_image_to_discord(focus_item_senkishoukaku,
                                    lastweek_21,
                                    today_21,
                                    graph_title=f"{last_week} ~ {today} 閃輝晶核価格推移")


@flow(log_prints=True)
def send_price_image(period: str = "1w"):
    u"""
    discordから受け取ったメッセージを解析して、価格推移のグラフを生成する
    メッセージは!グラフ <期間>
    期間は◯d, ◯w, ◯m, のいずれか
    """
    # 価格推移を表示
    # 現在の時刻を取得
    logger = get_run_logger()
    print(f"受け取った period: {period}（type: {type(period)}）")
    utc_now = datetime.now(timezone.utc)
    jst = pytz.timezone('Asia/Tokyo')
    jst_now = utc_now.astimezone(jst)
    today = jst_now.strftime("%Y-%m-%d")
    hour = jst_now.strftime("%H")
    # 価格情報を取得する日時を取得
    # periodを数字と単位に分離
    period_num = int(period[:-1])
    period_unit = period[-1]
    print(f"period_num: {period_num}（type: {type(period_num)}）")
    # 該当区間のデータを画像として送信する
    if period_unit == "d":
        # 日単位
        start_datetime = (jst_now - timedelta(days=period_num)).strftime("%Y-%m-%d %H:%M:%S")
        end_datetime = jst_now.strftime("%Y-%m-%d %H:%M:%S")
    elif period_unit == "w":
        # 週単位
        start_datetime = (jst_now - timedelta(weeks=period_num)).strftime("%Y-%m-%d %H:%M:%S")
        end_datetime = jst_now.strftime("%Y-%m-%d %H:%M:%S")
    elif period_unit == "m":
        # 月単位
        start_datetime = (jst_now - timedelta(days=period_num*30)).strftime("%Y-%m-%d %H:%M:%S")
        end_datetime = jst_now.strftime("%Y-%m-%d %H:%M:%S")
    else:
        raise ValueError("Invalid period unit. Use 'd', 'w', or 'm'.")
    # 輝晶核の価格推移を表示
    focus_item_kishoukaku = ["輝晶核", "魔因細胞", "魔因細胞のかけら"]
    # 閃輝晶核の価格推移を表示
    focus_item_senkishoukaku = ["閃輝晶核", "閃魔細胞", "閃魔細胞のかけら"]
    try:
        logger.info(f"受け取った period: {period}（type: {type(period)}）")
        send_price_image_to_discord(focus_item_kishoukaku,
                                    start_datetime,
                                    end_datetime,
                                    graph_title=f"{start_datetime} ~ {end_datetime} 輝晶核価格推移")
    except Exception as e:
        logger.error(f"✨ send_price_image_to_discord (kishoukaku) で例外発生: {e}")
    # 閃輝晶核の価格推移を表示
    try:
        send_price_image_to_discord(focus_item_senkishoukaku,
                                    start_datetime,
                                    end_datetime,
                                    graph_title=f"{start_datetime} ~ {end_datetime} 閃輝晶核価格推移")
    except Exception as e:
        logger.error(f"✨ send_price_image_to_discord (senkishoukaku) で例外発生: {e}")


@flow(log_prints=True)
def send_cheap_saibou_price():
    # discordからメッセージを受け取った際にやすい細胞の出品状況を通知する
    focus_item = ["魔因細胞", "魔因細胞のかけら", "閃魔細胞", "閃魔細胞のかけら"]
    # 価格情報を取得する日時を取得
    utc_now = datetime.now(timezone.utc)
    jst = pytz.timezone('Asia/Tokyo')
    jst_now = utc_now.astimezone(jst)
    today = jst_now.strftime("%Y-%m-%d")  # JSTの日付
    hour = jst_now.strftime("%H")
    # 価格情報を取得するタスクを作成
    df_all_price = pd.DataFrame()
    for item_name in focus_item:
        df_price = search_price(item_name)
        df_price = convert_to_iceberg_format(df_price,
                                             item_name,
                                             today,
                                             hour)
        # 価格情報を保存
        new_order = ["Name", "価格", "1つあたりの価格", "個数", "できのよさ",
                     "出品開始", "出品終了", "出品者", "Date", "Hour"]
        df_price = df_price[new_order]
        df_all_price = pd.concat([df_all_price, df_price],
                                 ignore_index=True)
        sleep(0.3)
    # やすい出品情報を取得
    send_cheap_saibou_price_to_discord(df_all_price)



if __name__ == "__main__":
    # get_price.serve(name="dqx-get-price")
    # get_price_hourly.serve(name="dqx-get-price-hourly")
    send_price_image.serve(name="dqx-send-price-image")
