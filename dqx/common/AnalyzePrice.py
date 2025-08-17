import pandas as pd
from common.trino_api import TrinoAPI


class AnalyzePrice:
    def __init__(self, table_name, datetime_range=None):
        self.trino = TrinoAPI(host='trino.mynet',
                              port=80,
                              user='tig',
                              catalog='iceberg')
        schema_name = 'dqx'
        self.df = self.trino.load(table_name, schema_name)
        self.df['Datetime'] = pd.to_datetime(self.df['date'] + ' ' + self.df['hour'].astype(str) + ':00:00')
        if datetime_range is not None:
            self.filter_by_datetime(datetime_range)

    def filter_by_datetime(self, datetime_range):
        """日時範囲でデータを絞り込む"""
        start_datetime = pd.to_datetime(datetime_range['start'])
        end_datetime = pd.to_datetime(datetime_range['end'])
        self.df = self.df[(self.df['Datetime'] >= start_datetime) & (self.df['Datetime'] <= end_datetime)]

    # 特定のアイテムの価格情報を取得
    def extract_item_price(self, item_name, quantity_threshold=None):
        # 特定のアイテムの価格情報を取得
        df = self.df[self.df['name'] == item_name]
        if quantity_threshold is not None:
            df = df[df['個数'] <= quantity_threshold]
        return df

    def min_price(self, item_name, quantity_threshold=None):
        # 最安の単価を返す
        df = self.extract_item_price(item_name)
        return df['1つあたりの価格'].min()

    def min_price_by_hour(self, item_name, quantity_threshold=None):
        # 時間ごとの最安価格を取得
        df = self.extract_item_price(item_name)
        return df.groupby('Datetime')['1つあたりの価格'].min()

    def percentile_price(self, item_name, percentile, quantity_threshold=None):
        # パーセンタイル価格を取得
        df = self.extract_item_price(item_name)
        return df['1つあたりの価格'].quantile(percentile)

    def percentile_price_by_hour(self, item_name, percentile, quantity_threshold=None):
        # 時間ごとのパーセンタイル価格を取得
        df = self.extract_item_price(item_name, quantity_threshold)
        return df.groupby('Datetime')['1つあたりの価格'].quantile(percentile)

    def average_price(self, item_name, quantity_threshold=None):
        # 平均価格を取得
        df = self.extract_item_price(item_name, quantity_threshold)
        return df['1つあたりの価格'].mean()

    # 1) 出来高加重平均 (VWAP)
    def vwap(self, item_name, q_threshold=None):
        df = self.extract_item(item_name, q_threshold)
        total = (df['unit_price'] * df['quantity']).sum()
        vol = df['quantity'].sum()
        return total / vol if vol else np.nan

    # 2) 1h 粒度ミニ集計（min/median/VWAP/volume）
    def hourly_summary(self, item_name, q_threshold=None):
        df = self.extract_item(item_name, q_threshold)
        g = df.groupby(level=0)
        return pd.DataFrame({
            'min': g['unit_price'].min(),
            'median': g['unit_price'].median(),
            'vwap': g.apply(lambda d: (d['unit_price']*d['quantity']).sum() /
                                      d['quantity'].sum()),
            'volume': g['quantity'].sum()
        })

    # 3) 移動平均とボラティリティ (window = 時間数)
    def rolling_stats(self, item_name, window=24, q_threshold=None):
        s = self.extract_item(item_name, q_threshold)['unit_price'] \
                .resample('1H').mean()
        return pd.DataFrame({
            'ma': s.rolling(window).mean(),
            'sigma': s.rolling(window).std()
        })

    # 4) 出来高 z-score → 異常検知
    def volume_zscore(self, item_name, window=168, q_threshold=None):
        vol = self.extract_item(item_name, q_threshold)['quantity'] \
                .resample('1H').sum()
        mu = vol.rolling(window).mean()
        std = vol.rolling(window).std()
        return (vol - mu) / std

    # 5) 強化期待コスト (成功確率を外部で与える)
    def expected_cost(self, item_name, success_prob=0.022, q_threshold=None):
        price = self.vwap(item_name, q_threshold)
        return price / success_prob if success_prob else np.nan







