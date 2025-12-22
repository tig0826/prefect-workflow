import seaborn as sns
import matplotlib.pyplot as plt
import japanize_matplotlib
import pandas as pd
import numpy as np
import datetime
from datetime import datetime, timedelta, timezone
import pytz

from common.trino_api import TrinoAPI
from common.discord_notify import send_to_discord
from common.AnalyzePrice import AnalyzePrice


def show_price_graph():
    utc_now = datetime.now(timezone.utc)
    jst = pytz.timezone('Asia/Tokyo')
    jst_now = utc_now.astimezone(jst)
    yesterday = (jst_now - timedelta(days=1)).replace(hour=21, minute=0, second=0, microsecond=0)
    datetime_yesterday = yesterday.strftime("%Y-%m-%d %H:%M:%S")
    today = jst_now.replace(hour=21, minute=0, second=0, microsecond=0)
    datetime_today = today.strftime("%Y-%m-%d %H:%M:%S")
    datetime_range_today = {"start": datetime_yesterday , "end": datetime_today}
    analyzer = AnalyzePrice("price_hourly", datetime_range_today)
    item_name = "輝晶核"
    df = analyzer.min_price_by_hour(item_name)
    df = df.reset_index()
    plt.figure(figsize=(12, 6))
    sns.lineplot(x="Datetime", y="1つあたりの価格", data=df, marker="o", linewidth=2.5)
    plt.title(f"{item_name} 本日の価格推移", fontsize=16)
    plt.xlabel("日時", fontsize=14)
    plt.ylabel("最安値", fontsize=14)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()
