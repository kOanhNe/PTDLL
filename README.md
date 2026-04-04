# PTDLL - Olist Lakehouse (Bronze / Silver / Gold)

## 1) Cấu trúc chuẩn

PTDLL/
├── docker/                 # Cấu hình Cluster (Hadoop, Spark, Hive, Superset)
│   ├── config/             # Các file core-site.xml, hdfs-site.xml...
│   ├── Dockerfile          # Image chuẩn cho toàn nhóm
│   └── start.sh            # Script khởi động dịch vụ
├── data/                   # Chứa 8 file .csv gốc (Lưu ý: Không push lên Git)
├── src/
│   ├── 01_brz/             # Tầng BRONZE: Ingestion dữ liệu thô
│   ├── 02_slv/             # Tầng SILVER: Cleaning & Transformation
│   └── 03_gld/             # Tầng GOLD: Aggregation & Business Insights
├── docker-compose.yml      # File điều phối toàn bộ hệ thống
├── .gitignore              # Chặn push dữ liệu nặng và rác hệ thống
└── README.md               # Hướng dẫn này

## 2) Chuẩn đường dẫn HDFS dùng chung
Tất cả script PySpark dùng prefix:

`hdfs://master:9000/lakehouse/`

## 3) Giai đoạn 1 - Setup đồng bộ
1. Chạy môi trường:
   - `docker compose up -d`
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
Tạo sẵn khung file trong `src/02_slv/` và `src/03_gld/` 

## 6) Trực quan
Kết nối Superset (Visualization)
Truy cập: http://localhost:8080.

Kết nối Database (SQLAlchemy URI):
hive://master:10001/default

Tạo Dataset từ các bảng có tiền tố gld_ để vẽ Dashboard.