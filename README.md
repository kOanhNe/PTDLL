# PTDLL - Olist Lakehouse (Bronze / Silver / Gold)

Project ETL Lakehouse chạy trên Hadoop + Spark + Hive + Superset.

# Nếu buil lại image nhiều:

docker builder prune -a -f

## 1) Cấu trúc chính

```text
hadooptrang/
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
docker compose up -d
```

### Bước 3 - Vào Bash

docker exec -it master bash

Khi thấy HDFS/YARN/Spark Thrift đã lên ổn thì sang bước tiếp.



### Bước 4 - Tạo thư mục Lakehouse trên HDFS

```bash
docker exec -it master hdfs dfsadmin -safemode leave || true
docker exec -it master hdfs dfs -mkdir -p /lakehouse/raw
docker exec -it master hdfs dfs -chmod -R 777 /lakehouse
```

### Bước 5 - Đẩy dữ liệu CSV lên HDFS

```bash
docker exec -it master bash -lc "hdfs dfs -put -f /data/*.csv /lakehouse/raw/"
docker exec -it master hdfs dfs -ls /lakehouse/raw/
```

> Lưu ý (macOS zsh): dùng `bash -lc` để wildcard `*.csv` được expand bên trong container.

### Bước 6 - Chạy Bronze

```bash
docker exec -it master spark-submit /app/src/01_brz/brz_ingest_all.py
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

## 4) Truy cập UI

- HDFS NameNode: http://localhost:9870
- YARN: http://localhost:8089
- Superset: http://localhost:8080
- Spark Thrift Server: `master:10001`

## 5) Kết nối Superset

Trong Superset, tạo database với SQLAlchemy URI:

`hive://master:10001/default`

Kiểm tra nhanh bảng đã đăng ký trong Hive Metastore:

```bash
beeline -u 'jdbc:hive2://master:10001/default' -n hive -e 'show tables;'
```

Sau đó tạo dataset từ các bảng Gold để vẽ dashboard.

## 6) Lỗi thường gặp

### `Name node is in safe mode`

```bash
hdfs dfsadmin -safemode leave
```

### Build image xong nhưng compose vẫn chạy image cũ

Build đúng tên image như compose đang dùng (`nhom17`):

```bash
docker build --no-cache -f docker/Dockerfile -t nhom17 .
```