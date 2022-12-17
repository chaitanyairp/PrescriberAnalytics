from pyspark.sql import SparkSession
from pyspark import SparkConf

import logging
import logging.config

logging.config.fileConfig(fname="..\\util\\config\\log_to_file.conf")
logger = logging.getLogger(__name__)

def get_spark_object(env):
    try:
        logger.info("Starting get_spark_object func")
        master = "local" if env == "TEST" else "yarn"
        spark_conf = SparkConf()
        spark_conf.set("spark.app.name", "Prescriber Analytics")
        spark_conf.set("spark.master", master)

        spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
    except Exception as exp:
        logger.error("Exception occured in get_spark_object", exc_info=True)
        raise
    else:
        logger.info("Returning spark object")
        return spark