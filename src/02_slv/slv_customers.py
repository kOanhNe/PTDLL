"""
SILVER LAYER - slv_customers.py

Người A - Core Entities: Customers

Xử lý:
1) Chuẩn hóa Zipcode:
   - Xóa khoảng trắng
   - Đảm bảo định dạng (không null)

2) Chuẩn hóa Thành phố:
   - Viết hoa chữ cái đầu mỗi từ
   - Xóa khoảng trắng dư thừa

3) Chuẩn hóa State:
   - Upper case (VD: sp -> SP)

4) Lọc Null, xóa trùng
5) Lưu Parquet

Chạy:
  spark-submit /app/src/02_slv/slv_customers.py
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, trim, upper, initcap, when, row_number, desc, 
    regexp_replace
)

# Cấu hình
HDFS_PREFIX = "hdfs://master:9000/lakehouse"
HDFS_BRZ = f"{HDFS_PREFIX}/brz"
HDFS_SLV = f"{HDFS_PREFIX}/slv"
SILVER_TABLE = "silver_customers"
SILVER_TABLE_PATH = f"{HDFS_SLV}/{SILVER_TABLE}"


def tao_spark() -> SparkSession:
    """Tạo SparkSession."""
    return (
        SparkSession.builder
        .appName("slv_customers")
        .config("spark.sql.adaptive.enabled", "true")
        .enableHiveSupport()
        .getOrCreate()
    )


def dang_ky_bang_hive_an_toan(spark: SparkSession, ten_bang: str, duong_dan: str) -> None:
    """Đăng ký bảng Hive, nếu lỗi thì chỉ cảnh báo để pipeline không bị dừng."""
    try:
        spark.sql(f"DROP TABLE IF EXISTS default.{ten_bang}")
        spark.sql(f"""
            CREATE TABLE default.{ten_bang}
            USING PARQUET
            LOCATION '{duong_dan}'
        """)
        print(f"  Đăng ký bảng Hive: default.{ten_bang}")
    except Exception as e:
        print(f"  Cảnh báo: Không đăng ký được bảng Hive default.{ten_bang} ({e})")
        print("  Dữ liệu Parquet vẫn sẵn sàng trong HDFS.")


def xu_ly_customers(spark: SparkSession) -> None:
    """
    Xử lý bảng Customers:
    - Chuẩn hóa Zipcode, City, State
    - Lọc null, xóa trùng
    - Lưu Silver
    """
    
    print("\nSILVER LAYER - Customers")
    
    # 1. Đọc Bronze
    print("\n[1/5] Đọc bronze_customers...")
    df_bronze = spark.read.parquet(f"{HDFS_BRZ}/bronze_customers")
    print(f"  Bronze: {df_bronze.count():,} dòng")
    
    # 2. Chuẩn hóa dữ liệu
    print("\n[2/5] Chuẩn hóa dữ liệu...")
    df = (
        df_bronze
        .withColumn("customer_id", col("customer_id"))
        .withColumn("customer_unique_id", col("customer_unique_id"))
        # Chuẩn hóa Zipcode: xóa khoảng trắng, upper case
        .withColumn("customer_zip_code_prefix",
                    trim(col("customer_zip_code_prefix")))
        # Chuẩn hóa City: xóa khoảng trắng dư, initcap (viết hoa chữ cái đầu)
        .withColumn("customer_city",
                    initcap(trim(col("customer_city"))))
        # Chuẩn hóa State: upper case
        .withColumn("customer_state",
                    upper(trim(col("customer_state"))))
    )
    
    # 3. Lọc Null
    print("\n[3/5] Lọc Null...")
    
    df_before = df.count()
    df = df.filter(
        col("customer_id").isNotNull() &
        col("customer_zip_code_prefix").isNotNull() &
        col("customer_city").isNotNull() &
        col("customer_state").isNotNull()
    )
    df_after = df.count()
    
    print(f"  Sau lọc Null: {df_before:,} -> {df_after:,} dòng")
    print(f"    Bỏ: {df_before - df_after:,} dòng (null values)")
    
    # 4. Xóa trùng lặp
    print("\n[4/5] Xóa trùng...")
    
    # Window: partition by customer_id -> keep first
    window = Window.partitionBy("customer_id").orderBy(desc("customer_unique_id"))
    df = (
        df
        .withColumn("row_num", row_number().over(window))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )
    
    print(f"  Sau xóa trùng: {df.count():,} dòng")
    
    # 5. Ghi Silver
    print("\n[5/5] Ghi vào silver_customers...")
    
    df.write.mode("overwrite").parquet(SILVER_TABLE_PATH)

    print(f"  Ghi thành công: {SILVER_TABLE_PATH}/")
    dang_ky_bang_hive_an_toan(spark, SILVER_TABLE, SILVER_TABLE_PATH)
    
    # Hiển thị schema
    print("\nSchema silver_customers")
    df.printSchema()
    
    # Hiển thị sample data
    print("\nSample Data")
    df.select(
        "customer_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state"
    ).limit(10).show(truncate=False)
    
    # Thống kê
    print("\nThống kê")
    print(f"Tổng customers: {df.count():,}")
    print(f"Số states: {df.select('customer_state').distinct().count()}")
    print(f"Số cities: {df.select('customer_city').distinct().count()}")
    
    print("\nTop 5 States:")
    df.groupBy("customer_state").count().orderBy(desc("count")).limit(5).show()
    
    print("\nTop 5 Cities:")
    df.groupBy("customer_city").count().orderBy(desc("count")).limit(5).show()


def main() -> None:
    spark = tao_spark()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        xu_ly_customers(spark)
        print("\nXong slv_customers")
    except Exception as e:
        print(f"\nLỗi: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
