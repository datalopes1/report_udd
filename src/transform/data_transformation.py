import duckdb
import pandas as pd
import logging


logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


def load_data(query_path: str, conn: duckdb.DuckDBPyConnection):
    """
    Lê um arquivo .SQL e executa a consulta usando uma conexão DuckDB.
    Retorna o resultado como DataFrame.

    Args:
        - query_path (str) - Caminho do arquivo .sql
        - conn (duckdb.DuckDBPyConnection) - Conexão com o banco de dados
    """
    try:
        logging.info(f"Lendo e executando consulta: {query_path}")
        with open(query_path, 'r') as f:
            query = f.read()
            return conn.execute(query).fetchdf()
    except Exception as e:
        logging.error(f"Erro ao executar a query {query_path}: {e}")
        raise


def save_table(df: pd.DataFrame,
               table_name: str,
               conn: duckdb.DuckDBPyConnection):
    """
    Salva um pd.DataFrame como tabela DuckDB

    Args:
        - df (pd.DataFrame) - DataFrame com os dados
        - table_name (str) - Nome da tabela que será salva no banco de dados
        - conn (duckdb.DuckDBPyConnection) - Conexão com o banco de dados
    """
    try:
        logging.info(f"Salvando a tabela '{table_name}' no banco de dados.")
        conn.register("tmp_df", df)
        conn.execute(
            f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM tmp_df"
        )
    except Exception as e:
        logging.info(f"Erro ao salvar a tabela '{table_name}': {e}")
        raise


if __name__ == "__main__":
    conn = duckdb.connect("data/db_imoveis.db")
    queries_path = "src/transform/queries"
    parquet_path = "data/processed"

    # Bronze
    bronze = load_data(f"{queries_path}/bronze.sql", conn)
    bronze = bronze.drop_duplicates()
    save_table(bronze, "bronze_imoveis", conn)
    bronze.to_parquet(f"{parquet_path}/bronze_imoveis.parquet", index=False)

    # Silver
    silver = load_data(f"{queries_path}/silver.sql", conn)
    silver = silver.drop_duplicates()
    save_table(silver, "silver_imoveis", conn)
    silver.to_parquet(f"{parquet_path}/silver_imoveis.parquet", index=False)

    # Gold
    gold = load_data(f"{queries_path}/gold.sql", conn)
    gold = gold.drop_duplicates()
    save_table(gold, "gold_imoveis", conn)
    gold.to_parquet(f"{parquet_path}/gold_imoveis.parquet", index=False)

    conn.close()
    logging.info("Arquivos no formato .parquet salvos em 'data/processed/'.")
    logging.info("Conexão encerrada.")