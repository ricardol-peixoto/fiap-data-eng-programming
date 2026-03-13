from config.settings import carregar_config
from session.spark_session import SparkSessionManager
from pipeline.pipeline import Pipeline
import logging


def configurar_logging():
    """Configura o logging para todo o projeto."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler("fiap-dataprg-trabfinal.log"),
            logging.StreamHandler(),
        ],
    )
    logging.info("Logging configurado.")


def main():
    config = carregar_config()[0]
    app_name = config["spark"]["app_name"]
    spark = None
    try:
        spark = SparkSessionManager.get_spark_session(app_name=app_name, config=config)
        pipeline = Pipeline(spark)
        pipeline.run(config=config)
    except Exception as e:
        logging.error(f"FALHA CRÍTICA NO PIPELINE: {e}")
    finally:
        if spark:
            spark.stop()
            logging.info("Sessão Spark finalizada.")


if __name__ == "__main__":
    configurar_logging()
    main()
