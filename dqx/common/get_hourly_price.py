from prefect import task
from common.DatetimeTranslator import DatetimeTranslator

@task(name="create hourly price message for discord",
      retries=5,
      retry_delay_seconds=1)
def create_message_kishoukaku_hourly_price(df_all):
    u"""discrodに送る価格情報のメッセージを作成する"""
    percentile = 0.05
    quantity_threshold = 3  # 一定個数以上の核の価格を排除。まとめ売りを排除するため
    # 価格情報を取得する日時を取得
    dt = DatetimeTranslator()
    datetime_now = dt.datetime()
    message = f"**{datetime_now} の価格情報**\n"
    message += "----------------------------------\n"
    focus_item_matrix = [["輝晶核", "魔因細胞", "魔因細胞のかけら"],
                         ["閃輝晶核", "閃魔細胞", "閃魔細胞のかけら"]]
    for forcus_item_list in focus_item_matrix:
        kaku_name, saibou_name, kakera_name = forcus_item_list
        for item_name in forcus_item_list:
            df_item = df_all[df_all["Name"] == item_name]
            if item_name in ["輝晶核", "閃輝晶核"]:
                # 核の場合は安く大量出品している人がいるので除外
                percentile_price = df_item[(df_item["個数"] <= quantity_threshold)]["1つあたりの価格"].quantile(percentile)
            else:
                percentile_price = df_item["1つあたりの価格"].quantile(percentile)
            message_price = f"**{item_name}** : " + "{:,}".format(int(percentile_price))+"G\n"
            message += message_price
        # 利益の期待値を計算
        # 細胞1個かかけら20個の内の安い方で計算
        price_saibou_base = min(df_all[df_all["Name"] == f"{saibou_name}"]["1つあたりの価格"].quantile(percentile),
                                df_all[df_all["Name"] == f"{kakera_name}"]["1つあたりの価格"].quantile(percentile)*20)
        # Nameが輝晶核で列名"個数"の値がquantity_threshold以上の核のデータを排除
        price_kaku_base = df_all[
                (df_all["Name"] == f"{kaku_name}") & (df_all["個数"] <= quantity_threshold)
                ]["1つあたりの価格"].quantile(percentile)
        profit = price_kaku_base*(1*0.1+(75/99)*0.5+(45/99)*0.4)*0.95*4 - price_saibou_base*30
        profit = int(profit)
        message_profit = "**一周持寄の利益期待値**: "+"{:,}".format(profit)+"G\n"
        message += message_profit
        message += "----------------------------------\n"
    return message
