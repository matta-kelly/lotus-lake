import trino
import os

def query_trino(sql: str):
    """
    Run a SQL query against the Trino container and return the results as a list of tuples.

    Trino connection defaults to:
      host = "trino"          (Docker service name)
      port = 8080
      catalog = "iceberg"
      schema = "mart"
      user = "dashboard"
    """

    host = os.getenv("TRINO_HOST", "trino")
    port = int(os.getenv("TRINO_PORT", "8080"))
    user = os.getenv("TRINO_USER", "dashboard")
    catalog = os.getenv("TRINO_CATALOG", "iceberg")
    schema = os.getenv("TRINO_SCHEMA", "mart")

    conn = trino.dbapi.connect(
        host=host,
        port=port,
        user=user,
        catalog=catalog,
        schema=schema,
    )
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows
