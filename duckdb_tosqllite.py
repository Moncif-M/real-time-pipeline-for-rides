import duckdb
import sqlite3

parquet_path = "/home/Viber/kafka_project/gold/rides_daily_summary_delta/*.parquet"

con = duckdb.connect()

df = con\
    .execute(f"SELECT * FROM '{parquet_path}'").df()


sqlite_conn = sqlite3\
    .connect("/opt/grafana/rides_data.db")

df.to_sql("rides_summary", sqlite_conn, if_exists="replace", index=False)

sqlite_conn.close()
