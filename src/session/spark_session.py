# src/session/spark_session.py
from pyspark.sql import SparkSession


class SparkSessionManager:
    """
    Gerencia a criação e o acesso à sessão Spark.
    """

    @staticmethod
    def get_spark_session(config, app_name: str = "alun-data-eng-pyspark-app") -> SparkSession:
        """
        Cria e retorna uma sessão Spark.

        :param app_name: Nome da aplicação Spark.
        :return: Instância da SparkSession.
        """
        return (SparkSession.builder.appName(app_name)
                .config("spark.driver.memory",       config["spark"]["driver_memory"])
                .config("spark.driver.cores",        config["spark"]["driver_cores"])
                .config("spark.executor.instances",  config["spark"]["executor_instances"])
                .config("spark.executor.memory",     config["spark"]["executor_memory"])
                .config("spark.executor.cores",      config["spark"]["executor_cores"])
                .config("spark.default.parallelism", config["spark"]["default_parallelism"])
                .getOrCreate())
