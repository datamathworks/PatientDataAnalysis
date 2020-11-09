from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

import re


def clean_data(data):
    """
       This function replaces invalid data with 0.

       Args:
           data (str): data to be verified and cleansed.

       Returns:
           number: The return value.
    """
    if data is None or data == "":
        return 0.0
    # matches any number (int, double etc.)
    result = bool(re.search("^[-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?$", data))

    if result is True:
        return float(data)
    else:
        return 0.0


if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName("Patient Data Analysis Application") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    df = spark.read.csv("/Users/mariusstratulat/PycharmProjects/PatientDataAnalysis"
                        "/com/python/patientdata/patient_data.csv",
                        header=True,
                        sep=",")
    df.printSchema()

    spark.udf.register("dataCleansing", clean_data)

    clean_udf = udf(clean_data, DoubleType())

    clean_df = df.withColumn("glucose_mg/dl_t1_cleansed", clean_udf("glucose_mg/dl_t1")) \
                 .withColumn("glucose_mg/dl_t2_cleansed", clean_udf("glucose_mg/dl_t2")) \
                 .withColumn("glucose_mg/dl_t3_cleansed", clean_udf("glucose_mg/dl_t3"))

    print(clean_df.collect())

