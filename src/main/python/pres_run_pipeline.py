from create_objects import get_spark_object
from validations import validate_spark_object
import get_variables as gav

import logging
import logging.config

logging.config.fileConfig(fname="..\\util\\config\\log_to_file.conf")
logger = logging.getLogger("root")


def main():
    try:
        logger.info("Started main()")
        env = gav.env
        spark = get_spark_object(env)
        validate_spark_object(spark)
        logger.info("Application ended.")
    except Exception as exp:
        logger.error("Error in main()", exc_info=True)


if __name__ == "__main__":
    main()