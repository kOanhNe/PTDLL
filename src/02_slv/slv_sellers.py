"""
SILVER LAYER - slv_sellers.py

Người A - Core Entities: Sellers

Xử lý (Tương tự Customers):
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
  spark-submit /app/src/02_slv/slv_sellers.py
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
SILVER_TABLE = "silver_sellers"
SILVER_TABLE_PATH = f"{HDFS_SLV}/{SILVER_TABLE}"


def tao_spark() -> SparkSession:
    """Tạo SparkSession."""
    return (
        SparkSession.builder
        .appName("slv_sellers")
        .config("spark.sql.adaptive.enabled", "true")
        .enableHiveSupport()
        .getOrCreate()
    )


def xu_ly_sellers(spark: SparkSession) -> None:
    """
    Xử lý bảng Sellers:
    - Chuẩn hóa Zipcode, City, State
    - Lọc null, xóa trùng
    - Lưu Silver
    """
    
    print("\nSILVER LAYER - Sellers")
    
    # 1. Đọc Bronze
    print("\n[1/5] Đọc bronze_sellers...")
    df_bronze = spark.read.parquet(f"{HDFS_BRZ}/bronze_sellers")
    print(f"  Bronze: {df_bronze.count():,} dòng")
    
    # 2. Chuẩn hóa dữ liệu
    print("\n[2/5] Chuẩn hóa dữ liệu...")
    df = (
        df_bronze
        .withColumn("seller_id", col("seller_id"))
        # Chuẩn hóa Zipcode: xóa khoảng trắng
        .withColumn("seller_zip_code_prefix",
                    trim(col("seller_zip_code_prefix")))
        # Chuẩn hóa City: xóa khoảng trắng dư, initcap (viết hoa chữ cái đầu)
        .withColumn("seller_city",
                    initcap(trim(col("seller_city"))))
        # Chuẩn hóa State: upper case
        .withColumn("seller_state",
                    upper(trim(col("seller_state"))))
    )
    
    # 3. Lọc Null
    print("\n[3/5] Lọc Null...")
    
    df_before = df.count()
    df = df.filter(
        col("seller_id").isNotNull() &
        col("seller_zip_code_prefix").isNotNull() &
        col("seller_city").isNotNull() &
        col("seller_state").isNotNull()
    )
    df_after = df.count()
    
    print(f"  Sau lọc Null: {df_before:,} -> {df_after:,} dòng")
    print(f"    Bỏ: {df_before - df_after:,} dòng (null values)")
    
    # 4. Xóa trùng lặp
    print("\n[4/5] Xóa trùng...")
    
    # Window: partition by seller_id -> keep first
    window = Window.partitionBy("seller_id").orderBy(desc("seller_city"))
    df = (
        df
        .withColumn("row_num", row_number().over(window))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )
    
    print(f"  Sau xóa trùng: {df.count():,} dòng")
    
    # 5. Ghi Silver
    print("\n[5/5] Ghi vào silver_sellers...")
    
    df.write.mode("overwrite").parquet(SILVER_TABLE_PATH)

    spark.sql(f"DROP TABLE IF EXISTS default.{SILVER_TABLE}")
    spark.sql(f"""
        CREATE TABLE default.{SILVER_TABLE}
        USING PARQUET
        LOCATION '{SILVER_TABLE_PATH}'
    """)

    print(f"  Ghi thành công: {SILVER_TABLE_PATH}/")
    print(f"  Đăng ký bảng Hive: default.{SILVER_TABLE}")
    
    # Hiển thị schema
    print("\nSchema silver_sellers")
    df.printSchema()
    
    # Hiển thị sample data
    print("\nSample Data")
    df.select(
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state"
    ).limit(10).show(truncate=False)
    
    # Thống kê
    print("\nThống kê")
    print(f"Tổng sellers: {df.count():,}")
    print(f"Số states: {df.select('seller_state').distinct().count()}")
    print(f"Số cities: {df.select('seller_city').distinct().count()}")
    
    print("\nTop 5 States:")
    df.groupBy("seller_state").count().orderBy(desc("count")).limit(5).show()
    
    print("\nTop 10 Cities:")
    df.groupBy("seller_city").count().orderBy(desc("count")).limit(10).show()


def main() -> None:
    spark = tao_spark()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        xu_ly_sellers(spark)
        print("\nXong slv_sellers")
    except Exception as e:
        print(f"\nLỗi: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
