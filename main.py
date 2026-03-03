from src.spark_session import get_spark_session
from src.log_settings import logger

def run_etl():
    with get_spark_session() as spark_session:
        logger.info("Hello World!")

if __name__ == "__main__":
    run_etl()