from src.spark_session import get_spark_session

def run_etl():
    with get_spark_session() as spark_session:
        print("Hello World!")

if __name__ == "__main__":
    run_etl()