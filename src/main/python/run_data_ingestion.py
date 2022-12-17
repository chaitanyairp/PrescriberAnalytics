import logging
import logging.config

logging.config.fileConfig("..\\util\\config\\log_to_file.conf")
logger = logging.getLogger(__name__)


def ingest_data_files(spark, file_name, file_path):
    try:
        logger.info(f"Starting data ingestion for file {file_name}")
        file_type = ""
        if file_name.endswith("parquet"):
            file_type = "parquet"
        if file_name.endswith("txt") or file_name.endswith("csv"):
            file_type = "csv"

        if file_type == "parquet":
            df = spark.read.format("parquet").option("path", file_path).load()
        if file_type == "csv":
            df = spark.read\
                .format("csv")\
                .option("header", True)\
                .option("inferSchema", True)\
                .option("path", file_path)\
                .load()
    except Exception as exp:
        logger.error("Error in ingest_data_files()", exc_info=True)
        raise
    else:
        logger.info(f"Ingestion completed for file {file_name}")
        return df
