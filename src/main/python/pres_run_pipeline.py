from create_objects import get_spark_object
from validations import validate_spark_object, print_df_schema, get_top_ten_records
from run_data_ingestion import ingest_data_files
from pres_data_preprocessing import dim_pre_processing, fact_pre_processing
import get_variables as gav



import os
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

        #load dim_city_file
        dim_fname = os.listdir(gav.dim_city_file_path)[0]
        dim_path = gav.dim_city_file_path + "\\" + dim_fname
        dim_city_df = ingest_data_files(spark, dim_fname, dim_path)
        dim_city_df = dim_pre_processing(dim_city_df)
        print_df_schema(dim_city_df, "dim_city_df")
        get_top_ten_records(dim_city_df, "dim_city_df")


        #load fact file
        fact_fname = os.listdir(gav.fact_file_path)[0]
        fact_path = gav.fact_file_path + "\\" + fact_fname
        fact_df = ingest_data_files(spark, fact_fname, fact_path)
        fact_df = fact_pre_processing(fact_df)
        print_df_schema(fact_df, "fact_df")
        get_top_ten_records(fact_df, "fact_df")

        logger.info("Application ended.")
    except Exception as exp:
        logger.error("Error in main()", exc_info=True)


if __name__ == "__main__":
    main()