import pandas as pd
import sqlite3
import logging

from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_data(data_path: str, zona: str):
    """
    Lê os arquivos no formato .JSON relacionados à zona da cidade de São Paulo/SP

    Args:
        data_path (str): Caminho dos dados brutos
        zona (str): Zona da cidade de São Paulo (Norte, Sul, Leste, Oeste)
    """
    zona = zona.lower()

    try:
        apt = pd.read_json(f"{data_path}/apt/zona_{zona}.json")
        casa = pd.read_json(f"{data_path}/casa/zona_{zona}.json")
        con = pd.read_json(f"{data_path}/con/zona_{zona}.json")
        logger.info(f"Dados zona {zona} carregados com sucesso.")

        df = pd.concat([apt, casa, con])
        logger.info("Dados concatenados com sucesso.")

        columns = df.drop(columns='uuid').columns.to_list()
        df.drop_duplicates(subset=columns, inplace=True)
        logger.info("Remoção de dados duplicados feita com sucesso.")
        logger.info(f"DataFrame com dados da zona {zona} criado com sucesso.")

        df['origem'] = "https://www.chavesnamao.com.br/ "
        df['ingestion_timestamp'] = datetime.now()
        return df

    except FileNotFoundError:
        logger.error(f"Erro: Arquivo não encontrado em '{data_path}'")
    except Exception as e:
        logger.error(f"Erro na ingestão dos dados da zona {zona}: {e}", exc_info=True)


if __name__ == "__main__":
    logger.info("Carregando dados dos imóveis.")
    zl = load_data("data/raw", "leste")
    zn = load_data("data/raw", "norte")
    zo = load_data("data/raw", "oeste")
    zs = load_data("data/raw", "sul")

    logger.info("Concatenando dados de todas as zonas")
    data = pd.concat([zl, zn, zo, zs])

    logger.info("Criando conexão com banco de dados sqlite.")
    conn = sqlite3.connect("data/database.db")

    logger.info("Ingestão de dados iniciada.")
    data.to_sql("raw_imoveis", conn, if_exists='replace', index=False, method='multi')

    conn.close()
    logger.info("Ingestão de dados concluída.")