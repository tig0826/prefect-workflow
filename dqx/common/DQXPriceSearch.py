from bs4 import BeautifulSoup
import pandas as pd
from time import sleep

from common.trino_api import TrinoAPI
from common.session_cookies import reuse_session


class DQXPriceSearch:
    def __init__(self):
        self.base_url = 'https://hiroba.dqx.jp/sc/search/bazaar'
        self.trino = TrinoAPI(host='trino.mynet',
                              port=80,
                              user='tig',
                              catalog='iceberg')
        # アイテム名とハッシュの対応表を取得
        self.df_weapon_hash = self.trino.load('metadata_weapon_hash', 'dqx')
        self.df_armor_hash = self.trino.load('metadata_armor_hash', 'dqx')
        self.df_dougu_hash = self.trino.load('metadata_dougu_hash', 'dqx')
        self.df_hash = pd.concat([self.df_weapon_hash,
                                  self.df_armor_hash,
                                  self.df_dougu_hash],
                                 ignore_index=True)
        # セッションを再利用
        self.session = reuse_session()

    def get_item_hash(self, item_name):
        item_hash = self.df_hash[self.df_hash['アイテム名'] == item_name]['ハッシュ']
        if len(item_hash) == 0:
            print(f"Item not found: {item_name}")
            return None
        return item_hash.values[0]

    def _search_price(self, item_name, page_num=0):
        # 出品情報を格納するリスト
        data = []
        item_hash = self.get_item_hash(item_name)
        url = f"{self.base_url}/{item_hash}/page/{page_num}"
        target_response = self.session.get(url)
        soup = BeautifulSoup(target_response.content, 'html.parser')
        if not target_response.ok:
            print("Failed to retrieve data with status code: ",
                  target_response.status_code)
        error_elements = soup.find_all(class_='txt_error')
        if len(error_elements) > 0:
            print(f"No items found for {item_name}")
            return None
        soup_tr = soup.find_all(class_='bazaarTable bazaarlist')[0]
        for row in soup_tr.find_all('tr')[1:]:  # 最初の行はヘッダーなのでスキップ
            cells = row.find_all('td')
            # できのよさ
            quality = cells[0].find('span', class_='starArea').text.strip()
            # 個数
            count = int(cells[1].find_all('p')[0].text.split('：')[1].rstrip('こ'))
            # 価格と1つあたりの価格
            price_info = cells[1].find_all('p')[1].text.split('\n')
            price = price_info[0].split('：')[1].replace('G', '').strip()
            price = int(price)
            unit_price = price_info[1].replace('(ひとつあたり', '').replace('G)', '').strip()
            unit_price = int(unit_price) if unit_price != '' else price
            # 出品者
            seller = cells[1].find('a', class_='strongLnk').text.strip()
            # 出品期間
            period = cells[2].text.strip()
            exhibit_start, exhibit_end = period.split(' ～ ')
            # データをリストに追加
            data.append([quality,
                         count,
                         price,
                         unit_price,
                         exhibit_start,
                         exhibit_end,
                         seller])
        return data

    def search_price(self, item_name):
        # 現在のページ番号
        pagenum = 0
        # 前回取得したページの情報の格納先
        last_page_content = ''
        # 出品情報を格納するデータフレーム
        while True:
            data = self._search_price(item_name, pagenum)
            if pagenum == 0:
                df = pd.DataFrame(data, columns=['できのよさ',
                                                 '個数',
                                                 '価格',
                                                 '1つあたりの価格',
                                                 '出品開始',
                                                 '出品終了',
                                                 '出品者'])
            else:
                df = pd.concat([df,
                                pd.DataFrame(data, columns=df.columns)],
                               ignore_index=True)
            if data is None:
                return df
            if data == last_page_content:
                return df
            last_page_content = data
            pagenum += 1
