"""
SILVER LAYER - slv_order_reviews.py

Người B - Product & Feedback: Order Reviews

Xử lý:
1) Lọc bỏ dữ liệu rác ở review_score (chỉ giữ 1-5 hoặc null)
2) Ép kiểu dữ liệu: review_score -> Int, timestamp -> Timestamp
3) Xử lý null ở review_comment_title, review_comment_message -> ""
4) Lọc Null cho các cột định danh quan trọng
5) Xóa trùng lặp (theo review_id, lấy review mới nhất)
6) Lưu Parquet

Chạy:
  spark-submit /app/src/02_slv/slv_order_reviews.py
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, trim, when, row_number, desc, to_timestamp, coalesce, lit, count, avg
)

HDFS_PREFIX = "hdfs://master:9000/lakehouse"
HDFS_BRZ = f"{HDFS_PREFIX}/brz"
HDFS_SLV = f"{HDFS_PREFIX}/slv"


def tao_spark() -> SparkSession:
    """Tạo SparkSession."""
    return (
        SparkSession.builder
        .appName("slv_order_reviews")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def xu_ly_order_reviews(spark: SparkSession) -> None:
    """
    Xử lý bảng Order Reviews:
    - Lọc dữ liệu rác
    - Ép kiểu dữ liệu (timestamp, review_score)
    - Xử lý null ở comments
    - Lọc, xóa trùng
    - Lưu Silver
    """
    print("\n[1/5] Đọc bronze_order_reviews...")
    df = spark.read.parquet(f"{HDFS_BRZ}/bronze_order_reviews")
    print(f"  Bronze: {df.count():,} dòng")
    
    print("\n[2/5] Lọc bỏ dữ liệu rác...")
    # Chỉ giữ review_score là 1-5, null, hoặc empty
    df_before = df.count()
    df = df.filter(
        (col("review_score").rlike("^[1-5]$")) | 
        (col("review_score").isNull()) |
        (col("review_score") == "")
    )
    df_after = df.count()
    print(f"  Loại bỏ: {df_before - df_after:,} dòng (review_score lỗi)")
    
    print("\n[3/5] Ép kiểu dữ liệu...")
    df = (
        df
        .withColumn("review_id", col("review_id"))
        .withColumn("order_id", col("order_id"))
        # Ép review_score thành INT (sau khi đã lọc bỏ dữ liệu rác)
        .withColumn("review_score", col("review_score").cast("int"))
        # Xử lý null ở comments -> ""
        .withColumn("review_comment_title",
                    coalesce(trim(col("review_comment_title")), lit("")))
        .withColumn("review_comment_message",
                    coalesce(trim(col("review_comment_message")), lit("")))
    )
    
    print("\n[4/5] Lọc dữ liệu timestamp rác trước khi cast...")
    # Chỉ giữ timestamp hợp lệ (format YYYY-MM-DD HH:MM:SS)
    df_before = df.count()
    df = df.filter(col("review_creation_date").rlike("^\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}$"))
    df_after = df.count()
    print(f"  Loại bỏ: {df_before - df_after:,} dòng (review_creation_date lỗi)")
    
    # Chuyển timestamp
    df = (
        df
        .withColumn("review_creation_date",
                    to_timestamp(col("review_creation_date")))
        .withColumn("review_answer_timestamp",
                    to_timestamp(col("review_answer_timestamp")))
    )
    
    print("\n[5/5] Thêm cột helper và lọc Null...")
    # Thêm cột helper: có comment không?
    df = (
        df
        .withColumn("has_comment",
                    when((col("review_comment_title") != "") | (col("review_comment_message") != ""), 1).otherwise(0))
    )
    
    df_before = df.count()
    df = df.filter(
        col("review_id").isNotNull() &
        col("order_id").isNotNull() &
        col("review_score").isNotNull()
    )
    df_after = df.count()
    print(f"  Sau lọc Null: {df_before:,} -> {df_after:,} dòng")
    
    print("\n[5/5] Xóa trùng...")
    window = Window.partitionBy("review_id").orderBy(desc("review_creation_date"))
    df = (
        df
        .withColumn("row_num", row_number().over(window))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )
    
    (
        df.write
        .mode("overwrite")
        .parquet(f"{HDFS_SLV}/silver_order_reviews")
    )
    print(f"\nGhi thành công: {HDFS_SLV}/silver_order_reviews/")
    
    print("\n--- THỐNG KÊ DỮ LIỆU ---")
    print(f"Tổng số Reviews: {df.count():,}")
    
    print("\n1. Phân bố Review Score:")
    df.groupBy("review_score").agg(count("*").alias("so_luong")).orderBy("review_score").show()
    
    print("2. Điểm đánh giá trung bình:")
    df.select(avg(col("review_score")).alias("avg_score")).show()


def main() -> None:
    spark = tao_spark()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        xu_ly_order_reviews(spark)
    except Exception as e:
        print(f"\nLỗi: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()