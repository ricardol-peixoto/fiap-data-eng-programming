"""Spark session utilities."""

from contextlib import contextmanager
from pyspark.sql import SparkSession
from src.config import spark_config
from src.log_settings import logger


class SparkSessionManager:
    """Manages Spark session creation and configuration."""

    _session = None

    @classmethod
    def get_session(cls) -> SparkSession:
        """Get or create Spark session."""
        if cls._session is None:
            cls._session = cls._create_session()
        return cls._session

    @classmethod
    def _create_session(cls) -> SparkSession:
        """Create a new Spark session with optimal configuration."""
        logger.info("Creating new Spark session")

        spark = (
            SparkSession.builder.appName(f"{spark_config.app_name}")
            .config("spark.driver.memory",       spark_config.driver_memory)
            .config("spark.driver.cores",        spark_config.driver_cores)
            .config("spark.executor.instances",  spark_config.executor_instances)
            .config("spark.executor.memory",     spark_config.executor_memory)
            .config("spark.executor.cores",      spark_config.executor_cores)
            .config("spark.default.parallelism", spark_config.default_parallelism)
            .config("spark.ui.port",             spark_config.ui_port)
            .getOrCreate())

        logger.info(f"Spark session created    : {spark_config.app_name}")
        logger.info(f"Spark version            : {spark.version}")
        logger.info(f"Spark driver memory      : {spark_config.driver_memory}")
        logger.info(f"Spark driver cores       : {spark_config.driver_cores}")
        logger.info(f"Spark executor instances : {spark_config.executor_instances}")
        logger.info(f"Spark executor cores     : {spark_config.executor_cores}")
        logger.info(f"Spark executor memory    : {spark_config.executor_memory}")
        return spark

    @classmethod
    def stop_session(cls):
        """Stop the Spark session."""
        if cls._session is not None:
            logger.info("Stopping Spark session")
            cls._session.stop()
            cls._session = None


@contextmanager
def get_spark_session():
    """Context manager for Spark session."""
    try:
        yield SparkSessionManager.get_session()
    finally:
        pass

