from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, datediff, round, when

spark = SparkSession.builder \
    .appName("Gold - Delivery Performance") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Đọc từ Hive Metastore (đồng bộ với Superset)
HDFS_SLV = "hdfs://master:9000/lakehouse/slv"
HDFS_GLD = "hdfs://master:9000/lakehouse/gld"


def doc_bang_hoac_parquet(spark: SparkSession, ten_bang: str, duong_dan_parquet: str):
    """Ưu tiên đọc Hive table; nếu lỗi thì fallback đọc Parquet để pipeline không dừng."""
    try:
        return spark.table(ten_bang)
    except Exception as e:
        print(f"Cảnh báo: Không đọc được {ten_bang} từ Hive ({e})")
        print(f"Fallback đọc Parquet: {duong_dan_parquet}")
        return spark.read.parquet(duong_dan_parquet)


def dang_ky_bang_hive_an_toan(spark: SparkSession, ten_bang: str, duong_dan: str) -> None:
    """Đăng ký bảng Hive, nếu lỗi thì chỉ cảnh báo để pipeline không bị dừng."""
    try:
        spark.sql(f"DROP TABLE IF EXISTS default.{ten_bang}")
        spark.sql(f"""
            CREATE TABLE default.{ten_bang}
            USING PARQUET
            LOCATION '{duong_dan}'
        """)
        print(f"Đăng ký bảng Hive: default.{ten_bang}")
    except Exception as e:
        print(f"Cảnh báo: Không đăng ký được bảng Hive default.{ten_bang} ({e})")
        print("Dữ liệu Parquet vẫn sẵn sàng trong HDFS.")

orders = doc_bang_hoac_parquet(spark, "default.silver_orders", f"{HDFS_SLV}/silver_orders")
reviews = doc_bang_hoac_parquet(spark, "default.silver_order_reviews", f"{HDFS_SLV}/silver_order_reviews")

# 5. Hiệu suất giao hàng
delivery = (
    orders
    .withColumn("delivery_status",
        when(col("order_delivered_customer_date") <= col("order_estimated_delivery_date"),
             "On Time").otherwise("Late"))
    .withColumn("delivery_days",
        datediff("order_delivered_customer_date", "order_purchase_timestamp"))
    .groupBy("delivery_status")
    .agg(count("order_id").alias("total_orders"),
         round(avg("delivery_days"), 1).alias("avg_delivery_days"))
)
delivery_path = f"{HDFS_GLD}/gold_delivery_performance"
delivery.write.mode("overwrite").parquet(delivery_path)
dang_ky_bang_hive_an_toan(spark, "gold_delivery_performance", delivery_path)
delivery.show()
print("gold_delivery_performance done")

# 6. Review score theo trạng thái giao hàng
review_score = (
    orders.join(reviews, "order_id", "left")
    .withColumn("delivery_status",
        when(col("order_delivered_customer_date") <= col("order_estimated_delivery_date"),
             "On Time").otherwise("Late"))
    .groupBy("delivery_status")
    .agg(round(avg("review_score"), 2).alias("avg_review_score"))
)
review_score_path = f"{HDFS_GLD}/gold_review_by_delivery"
review_score.write.mode("overwrite").parquet(review_score_path)
dang_ky_bang_hive_an_toan(spark, "gold_review_by_delivery", review_score_path)
review_score.show()
print("gold_review_by_delivery done")

print("\n🎉 gld_delivery_perf hoàn thành!")
spark.stop()