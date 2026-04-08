"""
SILVER LAYER - slv_orders.py

Người A - Core Entities: Orders

Xử lý:
1) Chuyển 5 cột ngày tháng từ String -> Timestamp:
   - order_purchase_timestamp
   - order_approved_at
   - order_delivered_carrier_date
   - order_delivered_customer_date
   - order_estimated_delivery_date

2) Tính thời gian giao hàng dự kiến vs thực tế:
   - delivery_days_planned = order_estimated_delivery_date - order_purchase_timestamp
   - delivery_days_actual = order_delivered_customer_date - order_purchase_timestamp
   - delivery_delay = delivery_days_actual - delivery_days_planned (nếu > 0 là trễ hạn)

3) Lọc Null, xóa trùng
4) Lưu Parquet

Chạy:
  spark-submit /app/src/02_slv/slv_orders.py
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, to_timestamp, datediff, when, coalesce, count,
    row_number, desc
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Cấu hình
HDFS_PREFIX = "hdfs://master:9000/lakehouse"
HDFS_BRZ = f"{HDFS_PREFIX}/brz"
HDFS_SLV = f"{HDFS_PREFIX}/slv"
SILVER_TABLE = "silver_orders"
SILVER_TABLE_PATH = f"{HDFS_SLV}/{SILVER_TABLE}"

# Định dạng timestamp Olist
TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"


def tao_spark() -> SparkSession:
    """Tạo SparkSession."""
    return (
        SparkSession.builder
        .appName("slv_orders")
        .config("spark.sql.adaptive.enabled", "true")
        .enableHiveSupport()
        .getOrCreate()
    )


def xu_ly_orders(spark: SparkSession) -> None:
    """
    Xử lý bảng Orders:
    - Chuyển cột ngày tháng sang Timestamp
    - Tính toán delay delivery
    - Lọc null, xóa trùng
    - Lưu Silver
    """

    print("\nSILVER LAYER - Orders")

    # 1. Đọc Bronze
    print("\n[1/5] Đọc bronze_orders...")
    df_bronze = spark.read.parquet(f"{HDFS_BRZ}/bronze_orders")
    print(f"  Bronze: {df_bronze.count():,} dòng")
    
    # 2. Chuyển đổi kiểu dữ liệu
    print("\n[2/5] Chuyển đổi kiểu dữ liệu...")
    df = (
        df_bronze
        .withColumn("order_id", col("order_id"))
        .withColumn("customer_id", col("customer_id"))
        .withColumn("order_status", col("order_status"))
        # Chuyển cột ngày tháng
        .withColumn("order_purchase_timestamp", 
                    to_timestamp(col("order_purchase_timestamp"), TIMESTAMP_FORMAT))
        .withColumn("order_approved_at",
                    to_timestamp(col("order_approved_at"), TIMESTAMP_FORMAT))
        .withColumn("order_delivered_carrier_date",
                    to_timestamp(col("order_delivered_carrier_date"), TIMESTAMP_FORMAT))
        .withColumn("order_delivered_customer_date",
                    to_timestamp(col("order_delivered_customer_date"), TIMESTAMP_FORMAT))
        .withColumn("order_estimated_delivery_date",
                    to_timestamp(col("order_estimated_delivery_date"), TIMESTAMP_FORMAT))
    )
    
    # 3. Tính toán delivery days
    print("\n[3/5] Tính toán thời gian giao hàng...")
    df = (
        df
        # Ngày dự kiến so với ngày mua
        .withColumn("delivery_days_planned",
                    datediff(col("order_estimated_delivery_date"), 
                            col("order_purchase_timestamp")))
        # Ngày thực tế so với ngày mua
        .withColumn("delivery_days_actual",
                    datediff(col("order_delivered_customer_date"),
                            col("order_purchase_timestamp")))
        # Delay: số ngày trễ hạn (âm = sớm, dương = trễ hạn)
        .withColumn("delivery_delay_days",
                    col("delivery_days_actual") - col("delivery_days_planned"))
        # Flag delivery delay
        .withColumn("is_delayed",
                    when(col("delivery_delay_days") > 0, True).otherwise(False))
    )
    
    # 4. Xử lý Null
    print("\n[4/5] Lọc Null, xóa trùng...")
    
    # Lọc những dòng có ngày delivery_customer_date (delivered orders)
    df = df.filter(col("order_delivered_customer_date").isNotNull())
    
    print(f"  Sau lọc Null: {df.count():,} dòng")
    
    # Xóa trùng lặp (keep first nếu có)
    window = Window.partitionBy("order_id").orderBy(desc("order_purchase_timestamp"))
    df = (
        df
        .withColumn("row_num", row_number().over(window))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )
    
    print(f"  Sau xóa trùng: {df.count():,} dòng")
    
    # 5. Ghi Silver
    print("\n[5/5] Ghi vào silver_orders...")
    
    df.write.mode("overwrite").parquet(SILVER_TABLE_PATH)

    print(f"  Ghi thành công: {SILVER_TABLE_PATH}/")

    try:
        spark.sql(f"DROP TABLE IF EXISTS default.{SILVER_TABLE}")
        spark.sql(f"""
            CREATE TABLE default.{SILVER_TABLE}
            USING PARQUET
            LOCATION '{SILVER_TABLE_PATH}'
        """)
        print(f"  Đăng ký bảng Hive: default.{SILVER_TABLE}")
    except Exception as e:
        print(f"  Cảnh báo: Không đăng ký được bảng Hive ({e})")
        print("  Silver vẫn được ghi Parquet bình thường.")
    
    # Hiển thị schema
    print("\nSchema silver_orders")
    df.printSchema()
    
    # Hiển thị sample data
    print("\nSample Data")
    df.select(
        "order_id",
        "order_status",
        "order_purchase_timestamp",
        "order_delivered_customer_date",
        "delivery_delay_days",
        "is_delayed"
    ).limit(5).show(truncate=False)
    
    # Thống kê
    print("\nThống kê")
    print(f"Tổng orders: {df.count():,}")
    
    delayed_count = df.filter(col("is_delayed") == True).count()
    print(f"Orders trễ hạn: {delayed_count:,} ({100*delayed_count/df.count():.1f}%)")
    
    df.select("delivery_delay_days").summary("min", "max", "mean").show()


def main() -> None:
    spark = tao_spark()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        xu_ly_orders(spark)
        print("\n Xong slv_orders")
    except Exception as e:
        print(f"\n Lỗi: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
