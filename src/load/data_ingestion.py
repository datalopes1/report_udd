import pandas as pd
import sqlite3
import logging

from datetime import datetime


logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


def load_data(file_path: str, origem: str):
    """
    Carrega os dados em formato .JSON e adiciona metadados

        Args:
        - file_path(str): Caminho do arquivo .JSON
        - origem(str): Site de origem dos dados
    """
    try:
        logging.info(f"Inicando o carregamento de '{file_path}'.")
        df = pd.read_json(file_path)
        df['origem'] = origem
        df['ingestion_timestamp'] = datetime.now()
        logging.info(f"Carregamento de '{file_path}' concluído.")
        return df
    except Exception as e:
        logging.error(f"Erro ao carregar '{file_path}': {e}", exc_info=True)
        raise


if __name__ == "__main__":
    dfs = [
        load_data(file_path="data/raw/apt.json", origem="Chaves na Mão"),
        load_data(file_path="data/raw/casas.json", origem="Chaves na Mão"),
        load_data(file_path="data/raw/con.json", origem="Chaves na Mão")
    ]

    data = pd.concat(dfs, ignore_index=True)

    conn = sqlite3.connect("data/database.db")
    logging.info("Iniciando a ingestão de dados.")
    data.to_sql("raw_imoveis", conn, if_exists='append', index=False)
    logging.info("Ingestão de dados concluída.")
    conn.close()
    logging.info(f"{len(data)} linhas inseridas.")
