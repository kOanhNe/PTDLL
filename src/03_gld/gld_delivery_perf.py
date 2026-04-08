from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, datediff, round, when

spark = SparkSession.builder \
    .appName("Gold - Delivery Performance") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Đọc từ Hive Metastore (đồng bộ với Superset)
HDFS_GLD = "hdfs://master:9000/lakehouse/gld"

orders = spark.table("default.silver_orders")
reviews = spark.table("default.silver_order_reviews")

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
spark.sql("DROP TABLE IF EXISTS default.gold_delivery_performance")
spark.sql(f"""
    CREATE TABLE default.gold_delivery_performance
    USING PARQUET
    LOCATION '{delivery_path}'
""")
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
spark.sql("DROP TABLE IF EXISTS default.gold_review_by_delivery")
spark.sql(f"""
    CREATE TABLE default.gold_review_by_delivery
    USING PARQUET
    LOCATION '{review_score_path}'
""")
review_score.show()
print("gold_review_by_delivery done")

print("\n🎉 gld_delivery_perf hoàn thành!")
spark.stop()