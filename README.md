# PTDLL - Olist Lakehouse (Bronze / Silver / Gold)

Project ETL Lakehouse chạy trên Hadoop + Spark + Hive + Superset.

# Script dành cho máy mới clone về chạy demo

# 1
```bash
docker compose up -d --build
```
# 2
```bash
docker exec -it prefect python flow.py
```
# HƯỚNG DẪN CHO CÁC THÀNH VIÊN
# Nếu buil lại image nhiều:
```bash
docker builder prune -a -f
```

## 1) Cấu trúc chính

```text
PTDLL/
├── flow.py
├── Docker-compose.yml
├── README.md
├── data/
├── docker/
│   ├── Dockerfile
│   ├── start.sh
│   └── config/
│       ├── core-site.xml
│       ├── hdfs-site.xml
│       ├── hive-site.xml
│       ├── mapred-site.xml
│       ├── spark-defaults.conf
│       ├── workers
│       └── yarn-site.xml
└── src/
    ├── 01_brz/
    ├── 02_slv/
    └── 03_gld/
```

## 2) Chuẩn đường dẫn

Tất cả script dùng chung prefix:

`hdfs://master:9000/lakehouse/`

## 3) Chạy từ đầu (từ bước build image)

### Bước 1 - Build image

> Image trong `Docker-compose.yml` đang dùng tên: `nhom17`

```bash
docker build --no-cache -f docker/Dockerfile -t nhom17 .
```

### Bước 2 - Khởi động cluster

```bash
docker compose down (không cần chạy nếu mới clone về)
docker compose up -d
```

## 🌟 3) Chạy tự động với Prefect (Khuyên dùng)

Thay vì chạy tay từng bước từ Bước 3 đến Bước 8, hệ thống đã tích hợp Prefect Orchestrator để tự động hóa toàn bộ quy trình qua Network (SSH).

### Bước 3.1: Thực thi toàn bộ Pipeline (End-to-End)

Lệnh này sẽ tự động tạo thư mục HDFS, nạp dữ liệu, và chạy lần lượt Bronze -> Silver -> Gold -> Superset

```bash
docker exec -it prefect python flow.py
```

### Bước 3.2: Theo dõi qua giao diện Web

Truy cập: http://localhost:4200 để xem biểu đồ và log xử lý trực quan.

---

## 🛠️ 4) Chạy thủ công (Dành cho Debug/Kiểm tra lẻ)

### Bước 3 - Vào Bash

```bash
docker exec -it master bash
```
Khi thấy HDFS/YARN/Spark Thrift đã lên ổn thì sang bước tiếp.

### Bước 4 - Tạo thư mục Lakehouse trên HDFS

```bash
hdfs dfs -mkdir -p /lakehouse/raw
hdfs dfs -chmod -R 777 /lakehouse
```

### Bước 5 - Đẩy dữ liệu CSV lên HDFS

```bash
hdfs dfs -put -f /data/*.csv /lakehouse/raw/
hdfs dfs -ls /lakehouse/raw/
```

### Bước 6 - Chạy Bronze

```bash
spark-submit /app/src/01_brz/brz_ingest_all.py
```

### Bước 7 - Chạy Silver

```bash
spark-submit /app/src/02_slv/slv_orders.py
spark-submit /app/src/02_slv/slv_order_items.py
spark-submit /app/src/02_slv/slv_customers.py
spark-submit /app/src/02_slv/slv_sellers.py
spark-submit /app/src/02_slv/slv_order_payments.py
spark-submit /app/src/02_slv/slv_order_reviews.py
spark-submit /app/src/02_slv/slv_products.py
```

### Bước 8 - Chạy Gold

```bash
spark-submit /app/src/03_gld/gld_sales_report.py
spark-submit /app/src/03_gld/gld_delivery_perf.py
```

## 5) Truy cập UI

- HDFS NameNode: http://localhost:9870
- YARN: http://localhost:8089
- Superset: http://localhost:8080
- Spark Thrift Server: `master:10001`

## 6) Kết nối Superset

### 5.1 Khởi tạo Superset lần đầu (bắt buộc)

Container `superset` trong compose hiện chỉ chạy web server, nên lần đầu cần init DB + tạo user admin thủ công.

```bash
docker exec -it superset superset db upgrade
docker exec -it superset superset fab create-admin \
    --username admin \
    --firstname Admin \
    --lastname User \
    --email admin@local.com \
    --password admin
docker exec -it superset superset init
```

Đăng nhập tại: `http://localhost:8080` với tài khoản vừa tạo.

### 5.2 trong Superset

Trong Superset:

1. Vào **Settings → Database Connections** (hoặc **Data → Databases** tùy version).
2. Chọn **+ DATABASE**.
3. Ở mục engine/chọn loại kết nối, chọn **Apache Spark SQL**.
4. Nhập URL (SQLAlchemy URI):

`hive://master:10001/default`

5. Bấm **Test Connection** → **Connect** (hoặc **Save**) khi pass.

> Nếu vẫn lỗi auth, thử URI: `hive://master:10001/default?auth=NOSASL`

Kiểm tra nhanh bảng đã đăng ký trong Hive Metastore:

```bash
docker exec -it master bash -lc "beeline -u 'jdbc:hive2://master:10001/default' -n hive -e 'show tables;'"
```

Sau đó vào **Data → Datasets → + Dataset**, chọn các bảng Gold để vẽ chart/dashboard.

## 7) Lỗi thường gặp

### `Name node is in safe mode`

```bash
docker exec -it master hdfs dfsadmin -safemode leave
```

### Build image xong nhưng compose vẫn chạy image cũ

Build đúng tên image như compose đang dùng (`nhom17`):

```bash
docker build --no-cache -f docker/Dockerfile -t nhom17 .
```
