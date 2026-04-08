"""
SILVER LAYER - slv_order_payments.py

Người B - Product & Feedback: Order Payments

Xử lý:
1) Ép kiểu dữ liệu: payment_value -> Float, payment_installments -> Int
2) Phân loại các phương thức thanh toán
   (credit_card -> Credit Card, debit_card -> Debit Card, boleto, voucher)
3) Xử lý lỗi dữ liệu installments (null/empty -> 1)
4) Lọc Null, lưu Parquet

Chạy:
  spark-submit /app/src/02_slv/slv_order_payments.py
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, trim, when, row_number, desc, coalesce, count, sum
)

HDFS_PREFIX = "hdfs://master:9000/lakehouse"
HDFS_BRZ = f"{HDFS_PREFIX}/brz"
HDFS_SLV = f"{HDFS_PREFIX}/slv"
SILVER_TABLE = "silver_order_payments"
SILVER_TABLE_PATH = f"{HDFS_SLV}/{SILVER_TABLE}"

def tao_spark() -> SparkSession:
    """Tạo SparkSession."""
    return (
        SparkSession.builder
        .appName("slv_order_payments")
        .config("spark.sql.adaptive.enabled", "true")
        .enableHiveSupport()
        .getOrCreate()
    )

def xu_ly_order_payments(spark: SparkSession) -> None:
    """
    Xử lý bảng Order Payments:
    - Ép kiểu dữ liệu
    - Phân loại payment_type
    - Lọc null
    - Lưu Silver
    """
    print("\n[1/4] Đọc bronze_order_payments...")
    df = spark.read.parquet(f"{HDFS_BRZ}/bronze_order_payments")
    
    # 2. Ép kiểu dữ liệu
    print("\n[2/4] Ép kiểu dữ liệu...")
    df = (
        df
        .withColumn("order_id", col("order_id"))
        .withColumn("payment_sequential", col("payment_sequential").cast("int"))
        .withColumn("payment_type", trim(col("payment_type")))
        .withColumn("payment_installments", 
                    when((col("payment_installments") == "") | col("payment_installments").isNull(), 1)
                    .otherwise(col("payment_installments").cast("int")))
        .withColumn("payment_value", col("payment_value").cast("float"))
    )
    
    print("\n[3/4] Phân loại payment_type...")
    payment_type_mapping = {
        "credit_card": "Credit Card",
        "debit_card": "Debit Card",
        "boleto": "Boleto",
        "voucher": "Voucher"
    }
    for old, new in payment_type_mapping.items():
        df = df.withColumn(
            "payment_type",
            when(col("payment_type").like(f"%{old}%"), new).otherwise(col("payment_type"))
        )
    
    df = df.filter(col("order_id").isNotNull() & col("payment_value").isNotNull())
    
    (
        df.write
        .format("parquet")
        .mode("overwrite")
        .option("path", SILVER_TABLE_PATH)
        .saveAsTable(f"default.{SILVER_TABLE}")
    )
    print(f"\nGhi thành công: {SILVER_TABLE_PATH}/")
    print(f"Đăng ký bảng Hive: default.{SILVER_TABLE}")
    
    print("\nTHỐNG KÊ")
    print(f"Tổng records: {df.count():,}")
    
    print("\nPhương thức thanh toán:")
    df.groupBy("payment_type").agg(
        count("payment_type").alias("count"),
        sum("payment_value").alias("total_value")
    ).orderBy(desc("count")).show(truncate=False)

def main() -> None:
    spark = tao_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        xu_ly_order_payments(spark)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()