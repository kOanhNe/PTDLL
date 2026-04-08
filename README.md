# PTDLL - Olist Lakehouse (Bronze / Silver / Gold)

Project ETL Lakehouse chạy trên Hadoop + Spark + Hive + Superset.

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

### Bước 3 - Chờ hệ thống sẵn sàng

```bash
docker compose -f Docker-compose.yml logs -f master
```

Khi thấy HDFS/YARN/Spark Thrift đã lên ổn thì sang bước tiếp.

### Bước 4 - Tạo thư mục Lakehouse trên HDFS

```bash
docker exec -it master hdfs dfsadmin -safemode leave || true
docker exec -it master hdfs dfs -mkdir -p /lakehouse/raw
docker exec -it master hdfs dfs -chmod -R 777 /lakehouse
```

### Bước 5 - Đẩy dữ liệu CSV lên HDFS

```bash
docker exec -it master hdfs dfs -put -f /data/*.csv /lakehouse/raw/
docker exec -it master hdfs dfs -ls /lakehouse/raw/
```

### Bước 6 - Chạy Bronze

```bash
docker exec -it master spark-submit /app/src/01_brz/brz_ingest_all.py
```

### Bước 7 - Chạy Silver

```bash
docker exec -it master spark-submit /app/src/02_slv/slv_orders.py
docker exec -it master spark-submit /app/src/02_slv/slv_order_items.py
docker exec -it master spark-submit /app/src/02_slv/slv_customers.py
docker exec -it master spark-submit /app/src/02_slv/slv_sellers.py
```

### Bước 8 - Chạy Gold

```bash
docker exec -it master spark-submit /app/src/03_gld/gld_sales_report.py
docker exec -it master spark-submit /app/src/03_gld/gld_delivery_perf.py
```

## 4) Truy cập UI

- HDFS NameNode: http://localhost:9870
- YARN: http://localhost:8089
- Superset: http://localhost:8080
- Spark Thrift Server: `master:10001`

## 5) Kết nối Superset

Trong Superset, tạo database với SQLAlchemy URI:

`hive://master:10001/default`

Sau đó tạo dataset từ bảng Gold để vẽ dashboard.

## 6) Lỗi thường gặp

### `Name node is in safe mode`

```bash
docker exec -it master hdfs dfsadmin -safemode leave
```

### `no configuration file provided: not found`

Dùng đúng file tên chữ hoa:

```bash
docker compose -f Docker-compose.yml up -d
```

### Build image xong nhưng compose vẫn chạy image cũ

Build đúng tên image như compose đang dùng (`nhom17`):

```bash
docker build --no-cache -f docker/Dockerfile -t nhom17 .
```