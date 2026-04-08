"""
SILVER LAYER - slv_order_items.py

Người A - Core Entities: Order Items

Xử lý:
1) Xử lý định dạng giá tiền (price, freight_value):
   - Cast String -> Double/Float
   - Lọc giá âm hoặc không hợp lệ

2) Kiểm tra khóa ngoại liên kết với Products:
   - Đảm bảo product_id không null
   - Flag nếu product_id tồn tại trong Products (nếu cần)

3) Chuyển order_item_id, product_id sang String
4) Lọc Null, xóa trùng
5) Lưu Parquet

Chạy:
  spark-submit /app/src/02_slv/slv_order_items.py
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, to_timestamp, cast, when, coalesce, count,
    row_number, desc
)
from pyspark.sql.types import DoubleType

# Cấu hình
HDFS_PREFIX = "hdfs://master:9000/lakehouse"
HDFS_BRZ = f"{HDFS_PREFIX}/brz"
HDFS_SLV = f"{HDFS_PREFIX}/slv"
SILVER_TABLE = "silver_order_items"
SILVER_TABLE_PATH = f"{HDFS_SLV}/{SILVER_TABLE}"

# Định dạng timestamp
TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"


def tao_spark() -> SparkSession:
    """Tạo SparkSession."""
    return (
        SparkSession.builder
        .appName("slv_order_items")
        .config("spark.sql.adaptive.enabled", "true")
        .enableHiveSupport()
        .getOrCreate()
    )


def xu_ly_order_items(spark: SparkSession) -> None:
    """
    Xử lý bảng Order Items:
    - Cast giá sang Double
    - Kiểm tra khóa ngoại
    - Lọc null, xóa trùng
    - Lưu Silver
    """
    
    print("\nSILVER LAYER - Order Items")
    
    # 1. Đọc Bronze
    print("\n[1/5] Đọc bronze_order_items...")
    df_bronze = spark.read.parquet(f"{HDFS_BRZ}/bronze_order_items")
    print(f"  Bronze: {df_bronze.count():,} dòng")
    
    # Hiển thị schema Bronze
    print("\n  Bronze schema:")
    df_bronze.printSchema()
    
    # 2. Chuyển đổi kiểu dữ liệu
    print("\n[2/5] Chuyển đổi kiểu dữ liệu...")
    df = (
        df_bronze
        .withColumn("order_id", col("order_id"))
        .withColumn("order_item_id", col("order_item_id").cast("integer"))
        .withColumn("product_id", col("product_id"))
        .withColumn("seller_id", col("seller_id"))
        # Timestamp
        .withColumn("shipping_limit_date",
                    to_timestamp(col("shipping_limit_date"), TIMESTAMP_FORMAT))
        # Cast price and freight to Double
        .withColumn("price", col("price").cast(DoubleType()))
        .withColumn("freight_value", col("freight_value").cast(DoubleType()))
    )
    
    # 3. Kiểm tra dữ liệu hợp lệ
    print("\n[3/5] Kiểm tra tính hợp lệ của dữ liệu...")
    
    # Lọc giá hợp lệ (>= 0)
    df_before = df.count()
    df = df.filter(
        (col("price").isNotNull()) & (col("price") >= 0) &
        (col("freight_value").isNotNull()) & (col("freight_value") >= 0)
    )
    df_after = df.count()
    
    print(f"  Lọc giá hợp lệ: {df_before:,} -> {df_after:,} dòng")
    print(f"    Bỏ: {df_before - df_after:,} dòng (giá âm/null)")
    
    # Kiểm tra khóa ngoại
    df_null_product = df.filter(col("product_id").isNull()).count()
    df_null_seller = df.filter(col("seller_id").isNull()).count()
    
    print(f"  Null product_id: {df_null_product:,} dòng")
    print(f"  Null seller_id: {df_null_seller:,} dòng")
    
    # Lọc khóa ngoại bắt buộc
    df = df.filter(
        col("product_id").isNotNull() &
        col("seller_id").isNotNull()
    )
    
    print(f"  Sau lọc khóa ngoại: {df.count():,} dòng")
    
    # 4. Xóa trùng lặp
    print("\n[4/5] Xóa trùng...")
    
    # Window: partition by (order_id, order_item_id) -> keep first
    window = Window.partitionBy("order_id", "order_item_id").orderBy(desc("shipping_limit_date"))
    df = (
        df
        .withColumn("row_num", row_number().over(window))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )
    
    print(f"  Sau xóa trùng: {df.count():,} dòng")
    
    # 5. Ghi Silver
    print("\n[5/5] Ghi vào silver_order_items...")
    
    (
        df.write
        .format("parquet")
        .mode("overwrite")
        .option("path", SILVER_TABLE_PATH)
        .saveAsTable(f"default.{SILVER_TABLE}")
    )

    print(f"  Ghi thành công: {SILVER_TABLE_PATH}/")
    print(f"  Đăng ký bảng Hive: default.{SILVER_TABLE}")
    
    # Hiển thị schema
    print("\nSchema silver_order_items")
    df.printSchema()
    
    # Hiển thị sample data
    print("\nSample Data")
    df.select(
        "order_id",
        "order_item_id",
        "product_id",
        "price",
        "freight_value"
    ).limit(5).show(truncate=False)
    
    # Thống kê
    print("\nThống kê")
    print(f"Tổng order items: {df.count():,}")
    
    df.select("price", "freight_value").summary("min", "max", "mean").show()


def main() -> None:
    spark = tao_spark()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        xu_ly_order_items(spark)
        print("\nXong slv_order_items")
    except Exception as e:
        print(f"\nLỗi: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
