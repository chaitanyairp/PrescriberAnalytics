import logging
import logging.config
from pyspark.sql import functions as F
from pyspark.sql.window import Window as W

logging.config.fileConfig(fname="..\\util\\config\\log_to_file.conf")
logger = logging.getLogger(__name__)


def dim_pre_processing(dim_df):
    # select required columns only
    # Convert cols to upper case
    try:
        logger.info("Started dim_pre_processing.")
        df = dim_df.select(
            F.upper(F.col("city")).alias("city"),
            "state_id",
            F.upper(F.col("state_name")).alias("state"),
            F.upper(F.col("county_name")).alias("county"),
            "zips"
        )
    except Exception as exp:
        logger.error("Error in dim_pre_processing ", exc_info=True)
        raise
    else:
        logger.info("Completed dim_pre_processing.")
        return df

def fact_pre_processing(fact_df):
    try:
        logger.info("Started fact_pre_processing")
        fact_sel_df = fact_df.select(
            F.col("npi").alias("pres_id"),
            F.col("nppes_provider_last_org_name").alias("pres_last_name"),
            F.col("nppes_provider_first_name").alias("pres_first_name"),
            F.col("nppes_provider_city").alias("pres_city"),
            F.col("nppes_provider_state").alias("pres_state"),
            F.col("specialty_description").alias("pres_speciality"),
            F.col("years_of_exp").alias("years_of_exp"),
            "drug_name",
            F.col("total_claim_count").alias("trx_claim_count"),
            "total_day_supply",
            "total_drug_cost"
        )

        fact_sel_df = fact_sel_df.withColumn("country", F.lit("USA"))

        fact_sel_df = fact_sel_df.withColumn("years_of_exp",F.regexp_extract(F.col("years_of_exp"), "(\d+)", 0))\
            .withColumn("years_of_exp", F.col("years_of_exp").cast("int"))

        fact_sel_df = fact_sel_df.withColumn("pres_name", F.concat(F.col("pres_last_name"), F.lit(" "), F.col("pres_first_name")))\
                .drop("pres_first_name", "pres_last_name")

        fact_sel_df = fact_sel_df.dropna(subset=["pres_id", "pres_name", "drug_name"])

        window_spec = W.partitionBy("pres_id")
        fact_sel_df = fact_sel_df.withColumn("avg_claim_count", F.avg("trx_claim_count").over(window_spec))
        fact_sel_df = fact_sel_df.withColumn("trx_claim_count", F.when(F.col("trx_claim_count").isNotNull(), F.col("trx_claim_count"))\
                                .otherwise(F.col("avg_claim_count")))

        # Printing null values count in dataframe.
        fact_sel_df.select(
            [F.count(F.when(F.col(c).isNull() | F.isnan(c), c)).alias(c) for c in fact_sel_df.columns]
        ).show()
    except Exception as exp:
        logger.error("Error in fact_pre_processing ", exc_info=True)
        raise
    else:
        logger.info("Completed fact_pre_processing")
        return fact_sel_df

# Select required cols
# Rename the columns
# Add a country field
# Clean years_of_Exp field
# convert years_of_exp to num
# Combine fname and lname
# Check and clean all null/NAN values
# Impute trc_cnt where it is null as avg of trx_cnt for prescriber