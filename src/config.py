import os
from dotenv import load_dotenv

load_dotenv()

class SparkConfig:
    """Spark configuration class."""

    def __init__(self):
        self.app_name            = os.getenv("SPARK_APP_NAME")
        self.log_level           = os.getenv("SPARK_LOG_LEVEL").upper()
        self.ui_port             = os.getenv("SPARK_UI_PORT")
        self.driver_cores        = os.getenv("SPARK_DRIVER_CORES")
        self.driver_memory       = os.getenv("SPARK_DRIVER_MEMORY")
        self.executor_instances  = os.getenv("SPARK_EXECUTOR_INSTANCES")
        self.executor_memory     = os.getenv("SPARK_EXECUTOR_MEMORY")
        self.executor_cores      = os.getenv("SPARK_EXECUTOR_CORES")
        self.default_parallelism = os.getenv("SPARK_DEFAULT_PARALLELISM")

spark_config = SparkConfig()