from common.AnalyzePrice import AnalyzePrice

@task(name="summarize price to create mart",
      retries=5,
      retry_delay_seconds=1)
def summarize_price(df, today, hour):
    u"""
    取得した価格情報を分析してmart化するために加工する
    """
    start_datetime = (jst_now - timedelta(days=period_num)).strftime("%Y-%m-%d %H:%M:%S")
    datetime_range = {"start": start_datetime, "end": end_datetime}
    analyze = AnalyzePrice("price_hourly", datetime_range)
