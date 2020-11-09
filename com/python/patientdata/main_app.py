from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, expr
from pyspark.sql.types import DoubleType, StringType

import re


class MainApp:

    def clean_data(self, data):
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

    def diabetes_indicator(self, glucose):
        """
            This function indicates the various indices of diabetes.

            Args:
               glucose (int): glucose index.

            Returns:
               str: The return value.
        """
        if glucose < 140:
            return "normal"
        elif 140 <= glucose <= 199:
            return "prediabetes"
        else:
            return "diabetes"

    def mask_data(self, data):
        """
           This function masks sensitive data.

           Args:
               data (str): data to be masked.

           Returns:
               str: The return value.
        """
        return "******"


if __name__ == '__main__':
    try:
        main_app = MainApp()
        spark = SparkSession \
            .builder \
            .appName("Patient Data Analysis Application") \
            .config("spark.some.config.option", "some-value") \
            .getOrCreate()

        df = spark.read.csv("/Users/mariusstratulat/PycharmProjects/PatientDataAnalysis"
                            "/com/python/patientdata/patient_data.csv",
                            header=True,
                            sep=",")

        spark.udf.register("dataCleansing", main_app.clean_data)

        clean_udf = udf(main_app.clean_data, DoubleType())

        clean_df = df.withColumn("dl_t1_cleansed", clean_udf("glucose_mg/dl_t1")) \
                     .withColumn("dl_t2_cleansed", clean_udf("glucose_mg/dl_t2")) \
                     .withColumn("dl_t3_cleansed", clean_udf("glucose_mg/dl_t3"))

        cols_list = ["dl_t1_cleansed", "dl_t2_cleansed", "dl_t3_cleansed"]
        expression = '+'.join(cols_list)

        avg_df = clean_df.withColumn("Average", expr(expression) / 3)

        spark.udf.register("diabetesIndicator", main_app.diabetes_indicator)

        diabetes_ind_udf = udf(main_app.diabetes_indicator, StringType())

        diabetes_indicator_df = avg_df.withColumn("diabetes_indicator", diabetes_ind_udf("Average"))

        spark.udf.register("maskData", main_app.mask_data)

        masked_data_udf = udf(main_app.mask_data, StringType())

        masked_df = diabetes_indicator_df.withColumn("Address", masked_data_udf("Address")) \
                                         .withColumn("lastName", masked_data_udf("lastName"))

        print(masked_df.collect())

        masked_df.write.mode('overwrite').parquet("output/patient_data.parquet")
    except RuntimeError:
        print("An error occurred")
