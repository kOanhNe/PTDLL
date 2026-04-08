from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, date_format, desc, round, sum

spark = SparkSession.builder \
    .appName("Gold - Sales Report") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Đọc trực tiếp từ HDFS
HDFS = "hdfs://master:9000/lakehouse/slv"
HDFS_GLD = "hdfs://master:9000/lakehouse/gld"

orders    = spark.read.parquet(f"{HDFS}/silver_orders")
items     = spark.read.parquet(f"{HDFS}/silver_order_items")
products  = spark.read.parquet(f"{HDFS}/silver_products")
customers = spark.read.parquet(f"{HDFS}/silver_customers")
payments  = spark.read.parquet(f"{HDFS}/silver_order_payments")

# 1. Doanh thu theo tháng
monthly_revenue = (
    orders.join(payments, "order_id", "left")
    .withColumn("month", date_format("order_purchase_timestamp", "yyyy-MM"))
    .groupBy("month")
    .agg(round(sum("payment_value"), 2).alias("total_revenue"),
         count("order_id").alias("total_orders"))
    .orderBy("month")
)
(
    monthly_revenue.write
    .format("parquet")
    .mode("overwrite")
    .option("path", f"{HDFS_GLD}/gold_monthly_revenue")
    .saveAsTable("default.gold_monthly_revenue")
)
monthly_revenue.show(5)
print("gold_monthly_revenue done")

# 2. Top 10 danh mục sản phẩm bán chạy
top_products = (
    items.join(products, "product_id", "left")
    .groupBy("product_category_name")
    .agg(count("order_id").alias("total_orders"),
         round(sum("price"), 2).alias("total_revenue"))
    .orderBy(desc("total_orders"))
    .limit(10)
)
(
    top_products.write
    .format("parquet")
    .mode("overwrite")
    .option("path", f"{HDFS_GLD}/gold_top_products")
    .saveAsTable("default.gold_top_products")
)
top_products.show()
print("gold_top_products done")

# 3. Doanh thu theo bang khách hàng
region_revenue = (
    orders.join(customers, "customer_id", "left")
    .join(payments, "order_id", "left")
    .groupBy("customer_state")
    .agg(count("order_id").alias("total_orders"),
         round(sum("payment_value"), 2).alias("total_revenue"))
    .orderBy(desc("total_revenue"))
)
(
    region_revenue.write
    .format("parquet")
    .mode("overwrite")
    .option("path", f"{HDFS_GLD}/gold_region_revenue")
    .saveAsTable("default.gold_region_revenue")
)
region_revenue.show()
print("gold_region_revenue done")

# 4. Phân tích phương thức thanh toán
payment_method = (
    payments.groupBy("payment_type")
    .agg(count("order_id").alias("total_orders"),
         round(sum("payment_value"), 2).alias("total_revenue"),
         round(avg("payment_installments"), 1).alias("avg_installments"))
    .orderBy(desc("total_revenue"))
)
(
    payment_method.write
    .format("parquet")
    .mode("overwrite")
    .option("path", f"{HDFS_GLD}/gold_payment_method")
    .saveAsTable("default.gold_payment_method")
)
payment_method.show()
print("gold_payment_method done")

print("\n🎉 gld_sales_report hoàn thành!")
spark.stop()