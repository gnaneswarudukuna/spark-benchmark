import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

def get_spark_session(app_name: str = None) -> SparkSession:
    load_dotenv()

    java_home = os.getenv("JAVA_HOME")
    if java_home:
        os.environ["JAVA_HOME"] = java_home
        java_bin = os.path.join(java_home, "bin")
        os.environ["PATH"] = java_bin + ":" + os.environ.get("PATH", "")

    master = os.getenv("SPARK_MASTER", "local[4]")
    name = app_name or os.getenv("SPARK_APP_NAME", "spark-benchmark")

    spark = (SparkSession.builder
             .master(master)
             .appName(name)
             .config("spark.sql.shuffle.partitions", "20")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())

    spark.sparkContext.setLogLevel("WARN")
    return spark
