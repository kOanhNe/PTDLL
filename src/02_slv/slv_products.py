"""
SILVER LAYER - slv_products.py

Người B - Product & Feedback: Products

Xử lý:
1) Đọc brz_products, xử lý Null ở các cột cân nặng, kích thước
2) Đọc brz_translation, làm sạch danh mục tiếng Anh
3) Join Products + Translation lấy tên tiếng Anh
4) Ép kiểu dữ liệu (weight, length, height, width -> Float)
5) Lọc Null, xóa trùng
6) Lưu Parquet

Chạy:
  spark-submit /app/src/02_slv/slv_products.py
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, trim, when, row_number, desc, coalesce, lit, count
)

HDFS_PREFIX = "hdfs://master:9000/lakehouse"
HDFS_BRZ = f"{HDFS_PREFIX}/brz"
HDFS_SLV = f"{HDFS_PREFIX}/slv"
SILVER_TABLE = "silver_products"
SILVER_TABLE_PATH = f"{HDFS_SLV}/{SILVER_TABLE}"


def tao_spark() -> SparkSession:
    """Tạo SparkSession."""
    return (
        SparkSession.builder
        .appName("slv_products")
        .config("spark.sql.adaptive.enabled", "true")
        .enableHiveSupport()
        .getOrCreate()
    )


def xu_ly_products(spark: SparkSession) -> None:
    """
    Xử lý bảng Products:
    - Join Products + Translation để có tên tiếng Anh
    - Ép kiểu dữ liệu (weight, length, height, width)
    - Lọc null, xóa trùng
    - Lưu Silver
    """
    print("\n--- SILVER LAYER - Products ---")
    
    print("\n[1/6] Đọc dữ liệu từ Bronze...")
    df_products = spark.read.parquet(f"{HDFS_BRZ}/bronze_products")
    df_translation = spark.read.parquet(f"{HDFS_BRZ}/bronze_translation")
    print(f"  Products: {df_products.count():,} dòng")
    print(f"  Translation: {df_translation.count():,} dòng")
    
    # 2. Chuẩn hóa dữ liệu Products
    print("\n[2/6] Chuẩn hóa cột và ép kiểu dữ liệu...")
    # Xử lý trọng lượng và kích thước: null/empty -> 0.0 (Yêu cầu Người B)
    df_products = (
        df_products
        .withColumn("product_name_lenght", 
                    when(col("product_name_lenght") == "", None).otherwise(col("product_name_lenght")).cast("int"))
        .withColumn("product_description_lenght", 
                    when(col("product_description_lenght") == "", None).otherwise(col("product_description_lenght")).cast("int"))
        .withColumn("product_photos_qty", 
                    when(col("product_photos_qty") == "", None).otherwise(col("product_photos_qty")).cast("int"))
        .withColumn("product_weight_g",
                    when((col("product_weight_g") == "") | col("product_weight_g").isNull(), 0.0)
                    .otherwise(col("product_weight_g").cast("float")))
        .withColumn("product_length_cm",
                    when((col("product_length_cm") == "") | col("product_length_cm").isNull(), 0.0)
                    .otherwise(col("product_length_cm").cast("float")))
        .withColumn("product_height_cm",
                    when((col("product_height_cm") == "") | col("product_height_cm").isNull(), 0.0)
                    .otherwise(col("product_height_cm").cast("float")))
        .withColumn("product_width_cm",
                    when((col("product_width_cm") == "") | col("product_width_cm").isNull(), 0.0)
                    .otherwise(col("product_width_cm").cast("float")))
    )
    
    df_translation = (
        df_translation
        .withColumn("product_category_name", trim(col("product_category_name")))
        .withColumn("product_category_name_english", trim(col("product_category_name_english")))
    )
    
    # 4. Join Products + Translation (Yêu cầu Người B phải join ngay)
    print("\n[3/6] Join Products với Translation lấy tên Tiếng Anh...")
    df = (
        df_products
        .join(
            df_translation.select(
                col("product_category_name"),
                col("product_category_name_english").alias("category_english")
            ),
            on="product_category_name",
            how="left"
        )
        # Nếu không có translation, giữ nguyên tên gốc (tránh mất category)
        .withColumn("product_category_name_english", 
                    coalesce(col("category_english"), col("product_category_name")))
        .drop("category_english")
    )
    
    # 5. Lọc và Xóa trùng (Deduplication)
    print("\n[4/6] Đang xử lý lọc Null và xóa trùng sản phẩm...")
    df = df.filter(col("product_id").isNotNull())
    
    window = Window.partitionBy("product_id").orderBy(desc("product_category_name"))
    df = (
        df
        .withColumn("row_num", row_number().over(window))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )
    
    print("\n[5/6] Đang ghi dữ liệu ra HDFS...")
    df.write.mode("overwrite").parquet(SILVER_TABLE_PATH)

    spark.sql(f"DROP TABLE IF EXISTS default.{SILVER_TABLE}")
    spark.sql(f"""
        CREATE TABLE default.{SILVER_TABLE}
        USING PARQUET
        LOCATION '{SILVER_TABLE_PATH}'
    """)
    print(f"  Ghi thành công: {SILVER_TABLE_PATH}/")
    print(f"  Đăng ký bảng Hive: default.{SILVER_TABLE}")
    
    print("\n[6/6] --- THỐNG KÊ KẾT QUẢ ---")
    print(f"  Tổng số sản phẩm (Sạch): {df.count():,}")
    
    so_categories = df.select("product_category_name").distinct().count()
    print(f"  Số lượng danh mục (Categories): {so_categories}")
    
    print("\nTop 10 Danh mục sản phẩm nhiều nhất (English):")
    df.groupBy("product_category_name_english") \
      .agg(count("*").alias("so_luong")) \
      .orderBy(desc("so_luong")) \
      .limit(10) \
      .show(truncate=False)


def main() -> None:
    spark = tao_spark()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        xu_ly_products(spark)
    except Exception as e:
        print(f"\nLỗi: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()