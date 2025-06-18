from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import IntegerType, StringType, BooleanType
from pyspark.sql.utils import AnalysisException

# === Initialize Spark Session ===
spark = SparkSession.builder.appName("ETL_Transactions_Cleanup").getOrCreate()

# === Define source and destination paths ===
input_path = "s3a://etl-data-source/transactions.csv"
output_path = "s3a://etl-data-transform/transactions_clean.parquet"

try:
    print(f"🔄 Reading CSV data from: {input_path}")
    raw_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(input_path)
    )

    print("📊 Initial Data Schema:")
    raw_df.printSchema()

    # === Data type casting and formatting ===
    transformed_df = (
        raw_df
        .withColumn("msno", col("msno").cast(StringType()))
        .withColumn("payment_method_id", col("payment_method_id").cast(IntegerType()))
        .withColumn("payment_plan_days", col("payment_plan_days").cast(IntegerType()))
        .withColumn("plan_list_price", col("plan_list_price").cast(IntegerType()))
        .withColumn("actual_amount_paid", col("actual_amount_paid").cast(IntegerType()))
        .withColumn("is_auto_renew", col("is_auto_renew").cast(BooleanType()))
        .withColumn("is_cancel", col("is_cancel").cast(BooleanType()))
        .withColumn("transaction_date", to_date(col("transaction_date").cast(StringType()), "yyyyMMdd"))
        .withColumn("membership_expire_date", to_date(col("membership_expire_date").cast(StringType()), "yyyyMMdd"))
        .na.drop()  # Drop rows with nulls
    )

    print(" Schema After Transformation:")
    transformed_df.printSchema()

    print(" Sample Cleaned Data:")
    transformed_df.show(5)

    print(f" Writing to Parquet at: {output_path}")
    transformed_df.write.mode("overwrite").parquet(output_path)

    print("Parquet file successfully written.")

except AnalysisException as ae:
    print(f" AnalysisException occurred: {ae}")

except Exception as e:
    print(f" Unexpected error: {e}")

finally:
    spark.stop()
