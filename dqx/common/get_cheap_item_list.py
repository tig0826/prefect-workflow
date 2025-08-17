def get_cheap_item_list(item_name,
                        df_all,
                        percentile=0.05,
                        quantity_threshold=10,
                        discount_margin=500,
                        top_n=15):
    u"""
    現在出品されているアイテムから、現在の相場の中で比較的安いものを抽出する
    - item_name: 対象のアイテム名。細胞の場合は欠片を20倍の価格に換算して同時に探索する。
    - df_all: 出品情報
    - percentile: 参考価格のパーセンタイル。これよりも大きく安いものを取得
    - quantity_threshold: 一定個数以下のかけらの価格を排除。安く出品されていても買わないため
    - discount_margin: 参考価格よりも安いものを取得するためのマージン
    - top_n: 安い順で表示する最大の件数
    """
    df = df_all[df_all["Name"] == item_name].copy()
    df["judge_price"] = df["1つあたりの価格"]
    five_percentile_price = df["judge_price"].quantile(percentile)
    if item_name in ["魔因細胞", "閃魔細胞"]:
        # 細胞の場合は欠片の価格を20倍したものも比較対象に含める
        item_name_kakera = item_name + "のかけら"
        df_kakera = df_all[df_all["Name"] == item_name_kakera].copy()
        # 欠片はしきい値よりも出品数が小さいものを除外
        df_kakera = df_kakera[df_kakera["個数"] > quantity_threshold]
        five_percentile_price_kakera = df_kakera["judge_price"].quantile(percentile)
        five_percentile_price = min(five_percentile_price, five_percentile_price_kakera)
    # 結合
    df_append = pd.concat([df, df_kakera], ignore_index=True)
    df_append = df_append.sort_values(by="1つあたりの価格", ascending=True)
    # しきい値よりも安いものを取得
    df_cheap = df_append[df_append["judge_price"] <= base_price - discount_margin]
    if len(df_cheap) < 3:
        # もし3件以下の場合は安い順に3件選ぶ
        df_cheap = df_append.head(3)
    else:
        # 安い順に10まで件選ぶ
        df_cheap = df_cheap.head(top_n)
    return df_cheap


def create_message_cheap_item(item_name, df_cheap):
    u"""
    抽出した安い出品情報からdiscordへ送るメッセージを作成する"""
    message = [f"🛒 **お手頃な{item_name}の出品状況**",
               "※ 冒険者の広場側でのデータ公開やdiscordへの通知までの遅延があります",
               "※ 参考程度に御覧ください..."]
    if item_name in ["魔因細胞", "閃魔細胞"]:
        message.append("※ 細胞の欠片は一定個数以上のまとめ売りのみを抽出しています。")
        message.append("-------------------------------")
        message.append(f"**{item_name}の出品情報**")
        # メッセージを作成
        for _, row in df_cheap.iterrows():
            item_name = row["Name"]
            unit_price = row["1つあたりの価格"]
            quantity = row["個数"]
            seller = row["出品者"]
            message.append(f"{item_name} : 単価**{unit_price:,}G** {quantity}個 出品者: {seller}")
    message = "\n".join(message)
    return message


