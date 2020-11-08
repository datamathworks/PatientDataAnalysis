from pyspark.sql import SparkSession


def print_hi():
    spark = SparkSession \
        .builder \
        .appName("Patient Data Analysis Application") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    df = spark.read.csv("/Users/mariusstratulat/PycharmProjects/PatientDataAnalysis"
                        "/com/python/patientdata/patient_data.csv",
                        header=True,
                        sep=",")

    print(df.collect())


if __name__ == '__main__':
    print_hi()

