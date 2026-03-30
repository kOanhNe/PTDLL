# Hướng dẫn code (dễ hiểu)

## Nguyên tắc
- Toàn bộ code trong thư mục `src/` dùng **Python**.
- Mỗi file chỉ làm **1 nhiệm vụ rõ ràng**.
- Đặt tên theo lớp dữ liệu:
  - `01_brz`: đọc và ghi Bronze
  - `02_slv`: làm sạch Silver
  - `03_gld`: tổng hợp Gold

## Cách chạy theo thứ tự
1. Bronze:
   - `python src/01_brz/brz_ingest_all.py`
   - hoặc `python src/01_brz/ingest_bronze.py`
2. Silver:
   - chạy từng file trong `src/02_slv/`
3. Gold:
   - chạy từng file trong `src/03_gld/`

## Mẫu code sườn (rất cơ bản)
```python
from pyspark.sql import SparkSession

HDFS_PREFIX = "hdfs://master:9000/lakehouse"

def build_spark() -> SparkSession:
    return SparkSession.builder.appName("ten_job").enableHiveSupport().getOrCreate()

def main() -> None:
    spark = build_spark()
    try:
        # 1) Read
        # 2) Transform
        # 3) Write
        pass
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
```
