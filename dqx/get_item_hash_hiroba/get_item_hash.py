import pandas as pd
from prefect import flow, task
from time import sleep
from pathlib import Path
import sys
from bs4 import BeautifulSoup

# プロジェクトのルートディレクトリを取得
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))
from common.session_cookies import load_session_cookies
from common.trino_api import TrinoAPI


@task(retries=30, retry_delay_seconds=1)
def search_item_hash(session, search_word):
    print(f"search {search_word}")
    url = f"https://hiroba.dqx.jp/sc/search/{search_word}/item"
    res = session.get(url)
    soup = BeautifulSoup(res.text, 'html.parser')
    table = soup.find('table', {'class': 'searchItemTable'}).tbody
    rows = table.find_all('tr')
    for row in rows:
        link_tag = row.find('a', class_='strongLnk')
        if link_tag and search_word == link_tag.text:
            href = link_tag['href']
            # ハッシュID部分のみを抽出
            hash_id = href.split('/')[-2]
            return hash_id
    # ハッシュIDが見つからなかった場合
    print(f"hash_id not found: {search_word}")
    return None


@flow(log_prints=True)
def get_item_hash():
    table_list = ["metadata_weapon", "metadata_armor", "metadata_dougu"]
    for table_name in table_list:
        session = load_session_cookies()
        exhibition_data = []
        trino = TrinoAPI(host='trino.mynet',
                         port=80,
                         user='tig',
                         catalog='iceberg')
        schema_name = "dqx"
        trino.create_schema(schema_name)
        df = trino.load(table_name, "dqx")
        table_name_hash = table_name + "_hash"
        if trino.table_exists(table_name_hash, schema_name):
            df_hash = trino.load(table_name_hash, "dqx")
        else:
            # 空のデータフレームを作成
            df_hash = pd.DataFrame(columns=["アイテム名", "カテゴリ", "ハッシュ"])
        # 各アイテムのハッシュIDを検索
        for _, row in df.iterrows():
            item_name = row["アイテム名"]
            # 既にハッシュIDが取得済みのアイテムはスキップ
            if item_name not in df_hash["アイテム名"].values:
                if item_name == "巨商の妻　かく語りき":
                    # 何故か検索に失敗するため、ハッシュIDを直接指定
                    item_category = row["カテゴリ"]
                    hash_code = "8e0c7280aebc796a7914afa0e007b91125f68f59"
                else:
                    item_category = row["カテゴリ"]
                    hash_code = search_item_hash(session, item_name)
                exhibition_data.append([item_name, item_category, hash_code])
                sleep(2)
        df_hash = pd.DataFrame(exhibition_data,
                               columns=["アイテム名", "カテゴリ", "ハッシュ"])
        trino.create_table(table_name_hash,
                           schema_name,
                           trino.extract_columns(df_hash))
        if len(df_hash) > 0:
            trino.insert_table(table_name_hash,
                               schema_name,
                               df_hash)


if __name__ == "__main__":
    get_item_hash.serve(name="dqx-get-item-hash")
