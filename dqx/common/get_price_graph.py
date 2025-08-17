from plotly.subplots import make_subplots
import plotly.graph_objects as go
import tempfile
from prefect import task

from common.AnalyzePrice import AnalyzePrice


@task(name="send price image to discord",
      retries=5,
      retry_delay_seconds=1)
def create_price_graph(focus_item, start_datetime, end_datetime, graph_title="価格推移"):
    u"""
    価格推移のグラフを生成し、一時ファイルに保存する。
    生成したグラフはPNG形式で保存され、ファイル名を返す。
    """
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
    return temp_file.name

