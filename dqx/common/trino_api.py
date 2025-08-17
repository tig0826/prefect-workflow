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
        conn = connect(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=self.catalog,
        )
        return conn

    def execute_query(self, query):
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute(query)
        df = cursor.fetchall()
        return df

    def table_exists(self, table_name, schema_name):
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute(f"SHOW TABLES FROM \"{schema_name}\"")
        tables = cursor.fetchall()
        tables = [table[0] for table in tables]
        return table_name in tables

    def load(self, table_name, schema_name):
        # 収集したデータを保存
        print(f'-- load table data {schema_name}.{table_name} ---')
        conn = self.connect()
        cursor = conn.cursor()
        sql_load = f"SELECT * FROM \"{schema_name}\".\"{table_name}\""
        cursor.execute(sql_load)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=columns)
        return df

    def extract_columns(self, df):
        pandas_to_trino_type = {
            "int64": "INTEGER",
            "float64": "DOUBLE",
            "object": "VARCHAR",
            "datetime64[ns]": "TIMESTAMP"
        }
        columns = []
        for col, dtype in df.dtypes.items():
            trino_type = pandas_to_trino_type.get(str(dtype), "VARCHAR")
            columns.append(f"\"{col}\" {trino_type}")
        table_columns = ",\n    ".join(columns)
        return table_columns

    def create_schema(self, schema_name):
        print(f'-- create schema {schema_name} ---')
        conn = self.connect()
        cursor = conn.cursor()
        sql_create_schema = f"""CREATE SCHEMA IF NOT EXISTS \"{schema_name}\"
        with (location = 's3a://iceberg/warehouse/{schema_name}')"""
        cursor.execute(sql_create_schema)

    def create_table(self,
                     table_name,
                     schema_name,
                     table_columns,
                     partitioning=None,
                     sorted_by=None):
        print(f'-- create table {schema_name}.{table_name} ---')
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
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute(create_table_query)

    def insert_table(self, table_name, schema_name, df, replace=False):
        print(f'-- insert to {schema_name}.{table_name} ---')
        conn = self.connect()
        cursor = conn.cursor()
        columns = ", ".join(f'"{col}"' for col in df.columns)
        values_list = []
        if replace:
            cursor.execute(f"DELETE FROM \"{schema_name}\".\"{table_name}\"")
        for _, row in df.iterrows():
            values = ", ".join(
                    f"'{str(v)}'" if isinstance(v, str)
                    else f"DATE '{str(v)}'" if isinstance(v, datetime.date)
                    else "NULL" if pd.isna(v)
                    else str(v)
                    for v in row
                    )
            values_list.append(f"({values})")
        insert_query = f"""
        INSERT INTO \"{schema_name}\".\"{table_name}\" ({columns})
        VALUES """ + ", \n".join(values_list)
        print(insert_query)
        cursor.execute(insert_query)
