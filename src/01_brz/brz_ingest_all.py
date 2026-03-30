"""
BRONZE LAYER - Đọc và Ghi dữ liệu

Nguyên tắc Bronze: "Đọc sao ghi vậy"
1) Đọc 8 file CSV từ HDFS (/lakehouse/raw/)
2) KHÔNG làm sạch, KHÔNG ép kiểu (tất cả cột = String)
3) Ghi Parquet vào /lakehouse/brz/{tên_bảng}/

Silver sẽ đọc từ /lakehouse/brz/ bằng:
  spark.read.parquet("hdfs://master:9000/lakehouse/brz/bronze_orders")

Chạy:
  spark-submit /app/src/01_brz/brz_ingest_all.py
"""

from pyspark.sql import SparkSession

# Cấu hình
HDFS_PREFIX = "hdfs://master:9000/lakehouse"
HDFS_RAW = f"{HDFS_PREFIX}/raw"
HDFS_BRZ = f"{HDFS_PREFIX}/brz"

# 8 file CSV
CAC_FILE_CSV = [
    ("olist_orders_dataset.csv", "bronze_orders"),
    ("olist_products_dataset.csv", "bronze_products"),
    ("olist_customers_dataset.csv", "bronze_customers"),
    ("olist_sellers_dataset.csv", "bronze_sellers"),
    ("olist_order_items_dataset.csv", "bronze_order_items"),
    ("olist_order_payments_dataset.csv", "bronze_order_payments"),
    ("olist_order_reviews_dataset.csv", "bronze_order_reviews"),
    ("product_category_name_translation.csv", "bronze_translation"),
]


def tao_spark() -> SparkSession:
    """Tạo SparkSession."""
    return (
        SparkSession.builder
        .appName("bronze_ingest_all")
        .getOrCreate()
    )


def doc_va_ghi_bronze(spark: SparkSession, stt: int, tong: int, ten_file: str, ten_bang: str) -> None:
    """
    Đọc 1 file CSV từ /lakehouse/raw/
    → Ghi Parquet vào /lakehouse/brz/{ten_bang}/
    
    Bronze: tất cả cột = String (inferSchema=False)
    Silver sẽ đọc từ /lakehouse/brz/ và tự ép kiểu.
    """
    duong_dan_csv = f"{HDFS_RAW}/{ten_file}"
    duong_dan_parquet = f"{HDFS_BRZ}/{ten_bang}"
    
    print(f"\n[{stt}/{tong}] {ten_file}")
    
    # Đọc CSV - tất cả cột = String
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("multiLine", "true")
        .option("encoding", "UTF-8")
        .csv(duong_dan_csv)
    )
    
    so_dong = df.count()
    so_cot = len(df.columns)
    print(f"  Đọc: {so_dong:,} dòng, {so_cot} cột")
    
    # Ghi Parquet
    (
        df.write
        .mode("overwrite")
        .parquet(duong_dan_parquet)
    )
    print(f"  Ghi: {duong_dan_parquet}/")


def main() -> None:
    spark = tao_spark()
    spark.sparkContext.setLogLevel("WARN")
    
    print("\nBRONZE LAYER - Ingest")
    
    tong = len(CAC_FILE_CSV)
    loi = []
    
    for stt, (ten_file, ten_bang) in enumerate(CAC_FILE_CSV, 1):
        try:
            doc_va_ghi_bronze(spark, stt, tong, ten_file, ten_bang)
        except Exception as e:
            print(f"  Lỗi: {e}")
            loi.append(ten_file)
    
    # Kết quả
    thanh_cong = tong - len(loi)
    print(f"\nKết quả: {thanh_cong}/{tong} bảng")
    
    if loi:
        print(f"Thất bại: {loi}")
    else:
        print("Thành công! Silver có thể đọc từ /lakehouse/brz/")
    
    spark.stop()


if __name__ == "__main__":
    main()