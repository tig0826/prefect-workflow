import os
from prefect import task
from common.trino_api import TrinoAPI


@task(name="Create External Table from Template")
def create_external(system_name: str, params: dict = None):
    """
    system_name: ドメイン名（例: "aw", "fitbit", "asken"）
    params: プレースホルダに埋め込む辞書（指定がなければそのまま実行）
    """
    # このファイル自身は常に `common/trino_tasks.py` にある。
    # つまり、2つ上の階層が必ず「プロジェクトのルートディレクトリ」になる！
    catalog_name = "hive"
    sql_file_path = os.path.join(
        f"{system_name}_exporter", "sql", "create_external.sql"
    )
    if not os.path.exists(sql_file_path):
        raise FileNotFoundError(f"No such file or directory: '{sql_file_path}'")
    with open(sql_file_path, "r", encoding="utf-8") as f:
        template = f.read()
    query = template.format(**params) if params else template
    query = query.strip().rstrip(";")
    api = TrinoAPI(host="trino.mynet", port=80, user="tig", catalog=catalog_name)
    print(f"🚀 Executing DDL for [{system_name}] from {sql_file_path}")
    api.execute_action(query)


@task(name="Sync Trino Partition", retries=3, retry_delay_seconds=10)
def sync_table_partition(table_name: str):
    """1つのテーブルのパーティションを同期するタスク"""
    catalog_name = "hive"
    schema_name = "life_bronze"
    api = TrinoAPI(
        host="trino.mynet",
        port=80,
        user="tig",
        catalog=catalog_name,
    )
    query = f"CALL hive.system.sync_partition_metadata('{schema_name}', '{table_name}', 'ADD')"
    print(f"Executing: {query}")
    api.execute_action(query)
    print(f"{schema_name}.{table_name} のパーティション同期完了！")
