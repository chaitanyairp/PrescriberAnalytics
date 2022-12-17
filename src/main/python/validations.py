import logging
import logging.config
import pandas

logging.config.fileConfig(fname="..\\util\\config\\log_to_file.conf")
logger = logging.getLogger(__name__)


def validate_spark_object(spark):
    try:
        logger.info("Starting validate_spark_object().")
        dt = spark.sql("select current_date()")
    except Exception as exp:
        logger.error("Exception in validate_spark_object ", exc_info=True)
        raise
    else:
        logger.info("validating spark object by printing current_date " + str(dt.collect()))

def get_top_ten_records(df, df_name):
    try:
        logger.info(f"Printing top 10 records for df {df_name}")
        top_rec = df.limit(10)
        df_pandas =top_rec.toPandas()
        logger.info('\n \t' + df_pandas.to_string(index=False))
    except Exception as exp:
        logger.error(f"Exception in getting top 10 records for df {df_name}", exc_info=True)
    else:
        logger.info(f"Done printing top 10 records for df {df_name}")


def print_df_schema(df, df_name):
    try:
        logger.info(f"Printing schema for dataframe {df_name}")
        for field in df.schema:
            logger.info(f"\t{field}")
    except Exception as exp:
        logger.error(f"Error in printing schema for df {df_name}", exc_info=True)
        raise
    else:
        logger.info(f"Completed printing schema for df {df_name}")

