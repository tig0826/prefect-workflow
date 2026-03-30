import contextlib
import pandas as pd
from trino.dbapi import connect
import datetime


class TrinoAPI:
    def __init__(self, host, port, user, catalog):
        self.host = host
        self.port = port
        self.user = user
        self.catalog = catalog

    def connect(self):
        """Trinoへの接続オブジェクトを返す（HTTPセッションの準備）"""
        conn = connect(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=self.catalog,
        )
        return conn

    def execute_query(self, query):
        """汎用的なSELECTクエリを実行し、DataFrameで返す"""
        with contextlib.closing(self.connect()) as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(rows, columns=columns)
            return df

    def execute_action(self, query):
        """INSERTやCREATEなど、結果(DataFrame)を返さないクエリを実行する"""
        with contextlib.closing(self.connect()) as conn:
            cursor = conn.cursor()
            cursor.execute(query)

    def execute_action(self, query):
        """INSERTやCREATEなど、結果を返さないクエリを実行する"""
        with contextlib.closing(self.connect()) as conn:
            cursor = conn.cursor()
            cursor.execute(query)

    def table_exists(self, table_name, schema_name):
        with contextlib.closing(self.connect()) as conn:
            cursor = conn.cursor()
            cursor.execute(f'SHOW TABLES FROM "{schema_name}"')
            tables = cursor.fetchall()
            tables = [table[0] for table in tables]
            return table_name in tables

    def load(self, table_name, schema_name):
        print(f"-- load table data {schema_name}.{table_name} ---")
        sql_load = f'SELECT * FROM "{schema_name}"."{table_name}"'
        return self.execute_query(sql_load)

    def extract_columns(self, df):
        pandas_to_trino_type = {
            "int64": "INTEGER",
            "float64": "DOUBLE",
            "object": "VARCHAR",
            "datetime64[ns]": "TIMESTAMP",
        }
        columns = []
        for col, dtype in df.dtypes.items():
            trino_type = pandas_to_trino_type.get(str(dtype), "VARCHAR")
            columns.append(f'"{col}" {trino_type}')
        table_columns = ",\n    ".join(columns)
        return table_columns

    def create_schema(self, schema_name):
        print(f"-- create schema {schema_name} ---")
        sql_create_schema = f"""CREATE SCHEMA IF NOT EXISTS "{schema_name}"
        with (location = 's3a://iceberg/warehouse/{schema_name}')"""
        self.execute_action(sql_create_schema)

    def create_table(
        self, table_name, schema_name, table_columns, partitioning=None, sorted_by=None
    ):
        print(f"-- create table {schema_name}.{table_name} ---")
        with_options = ["format = 'PARQUET'"]
        if partitioning:
            partitioning_str = "', '".join(partitioning)
            with_options.append(f"partitioning = ARRAY['{partitioning_str}']")
        if sorted_by:
            sorted_by_str = "', '".join(sorted_by)
            with_options.append(f"sorted_by = ARRAY['{sorted_by_str}']")
        with_clause = ",\n    ".join(with_options)
        create_table_query = f"""CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        {table_columns}
        )
        WITH (
        {with_clause}
        )
        """
        print(f"Generated query:\n{create_table_query}")
        self.execute_action(create_table_query)

    def _to_trino_literal(self, v):
        # 変更なしのため省略（お前の書いた完璧なロジックをそのまま使え）
        if v is None or (isinstance(v, float) and pd.isna(v)) or pd.isna(v):
            return "NULL"
        if isinstance(v, (pd.Timestamp, datetime.datetime)):
            ts = v
            if getattr(ts, "tzinfo", None) is not None:
                try:
                    ts = ts.tz_convert("Asia/Tokyo").tz_localize(None)
                except Exception:
                    JST = datetime.timezone(datetime.timedelta(hours=9))
                    ts = ts.astimezone(JST).replace(tzinfo=None)
            return f"TIMESTAMP '{ts.strftime('%Y-%m-%d %H:%M:%S.%f')}'"
        if isinstance(v, datetime.date) and not isinstance(v, datetime.datetime):
            return f"DATE '{v.isoformat()}'"
        if isinstance(v, str):
            return "'" + v.replace("'", "''") + "'"
        if isinstance(v, bool):
            return "TRUE" if v else "FALSE"
        return str(v)

    def insert_table(self, table_name, schema_name, df, replace=False, chunk_size=1000):
        print(f"-- insert to {schema_name}.{table_name} ---")
        columns = ", ".join(f'"{col}"' for col in df.columns)

        # 1回のINSERT処理全体を1つのコネクションでカバーする
        with contextlib.closing(self.connect()) as conn:
            cursor = conn.cursor()

            if replace:
                cursor.execute(f'DELETE FROM "{schema_name}"."{table_name}"')

            for i in range(0, len(df), chunk_size):
                chunk = df.iloc[i : i + chunk_size]
                values_list = []
                for _, row in chunk.iterrows():
                    values = ", ".join(self._to_trino_literal(v) for v in row)
                    values_list.append(f"({values})")
                insert_query = f"""
                INSERT INTO "{schema_name}"."{table_name}" ({columns})
                VALUES {", ".join(values_list)}
                """
                cursor.execute(insert_query)
                print(f"Inserted chunk {i} to {i + len(chunk)}")
