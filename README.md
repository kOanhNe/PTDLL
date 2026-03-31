# PTDLL - Olist Lakehouse (Bronze / Silver / Gold)

## 1) Cấu trúc chuẩn
- docker/
- data/
- src/01_brz/
- src/02_slv/
- src/03_gld/
- notebooks/

## 2) Chuẩn đường dẫn HDFS dùng chung
Tất cả script PySpark dùng prefix:

`hdfs://master:9000/lakehouse/`

## 3) Giai đoạn 1 - Setup đồng bộ
1. Chạy môi trường:
   - `docker compose -f docker/docker-compose.yml up -d`
2. Đẩy dữ liệu Olist lên HDFS (mỗi máy tự đẩy):
   - `hdfs dfs -mkdir -p /lakehouse/raw`
   - `hdfs dfs -put -f /path/to/*.csv /lakehouse/raw/`

## 4) Giai đoạn 2 - Bronze (đọc sao ghi vậy)
Script Bronze:
- `src/01_brz/ingest_bronze.py`
- `src/01_brz/brz_ingest_all.py` (wrapper chạy nhanh)

Chạy trong container master:
- `spark-submit /path/to/src/01_brz/ingest_bronze.py`

Kết quả tạo các bảng:
- `bronze_orders`
- `bronze_products`
- `bronze_customers`
- `bronze_sellers`
- `bronze_order_items`
- `bronze_order_payments`
- `bronze_order_reviews`
- `bronze_translation`

> Bronze không làm sạch dữ liệu, chỉ ingest và đổi tên chuẩn.

## 5) Silver và Gold
Đã tạo sẵn khung file trong `src/02_slv/` và `src/03_gld/` theo đúng yêu cầu cấu trúc.
