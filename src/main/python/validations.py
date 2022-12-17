import logging
import logging.config

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
