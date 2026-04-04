# Airflow + Superset Setup Guide

## **Bước 1: Start Airflow + Superset**

```bash
cd D:\PTDLL
docker-compose -f docker-compose-with-airflow.yml up -d
```

Chờ ~2 phút để containers khởi động.

## **Bước 2: Truy cập Airflow Webserver**

- URL: **http://localhost:8081**
- Username: `admin`
- Password: `admin`

Các DAG sẽ được tự động load từ `./src/` folder.

### Cách trigger DAG thủ công:
1. Vào DAG "ptdll_medallion_pipeline"
2. Click "Trigger DAG" (nút xanh)
3. Chọn ngày execute
4. Monitor tất cả tasks chạy lần lượt

---

## **Bước 3: Setup Superset + Kết nối Spark**

### Vào Superset:
- URL: **http://localhost:8080**
- Chỉnh sửa cấu hình nếu cần

### Kết nối Database (Spark Thrift Server):

1. **Vào Settings → Database Connections**
2. **Click "+ Database" (thêm mới)**
3. Chọn **"Apache Spark"** hoặc **"Hive"**
4. Điền thông tin:
   ```
   Host: master
   Port: 10001
   Database: default
   ```
5. **Click "Test Connection"**
6. **Lưu (Save)**

---

## **Bước 4: Tạo Dataset từ Gold Tables**

1. **Vào Datasets (thêm dữ liệu)**
2. **Click "+ Dataset"**
3. Chọn Database → Spark
4. Chọn các bảng Gold:
   - `gold_monthly_revenue` (từ gld_sales_report)
   - `gold_top_products` (từ gld_sales_report)
   - `gold_delivery_perf` (từ gld_delivery_perf)

---

## **Bước 5: Tạo Dashboard**

### Tạo Chart 1 - Monthly Revenue:
1. **Click "+ Chart"**
2. **Chọn Dataset: gold_monthly_revenue**
3. **Visualization Type: Line Chart**
4. **X-axis: month**
5. **Y-axis: total_revenue**
6. **Title: Monthly Revenue Trend**
7. **Save Chart**

### Tạo Chart 2 - Top Products:
1. **Click "+ Chart"**
2. **Chọn Dataset: gold_top_products**
3. **Visualization Type: Bar Chart**
4. **X-axis: product_category_name**
5. **Y-axis: total_orders**
6. **Title: Top 10 Product Categories**
7. **Save Chart**

### Tạo Chart 3 - Delivery Performance:
1. **Click "+ Chart"**
2. **Chọn Dataset: gold_delivery_perf**
3. **Visualization Type: Pie Chart hoặc KPI**
4. **Metric: on_time_rate / late_rate**
5. **Title: Delivery Performance**
6. **Save Chart**

### Tạo Dashboard:
1. **Vào Dashboards → "+ Dashboard"**
2. **Đặt tên: "PTDLL Olist Analytics"**
3. **Edit Mode → Kéo thả 3 charts vào**
4. **Sắp xếp layout**
5. **Lưu Dashboard**

---

## **Bước 6: Export Dashboard Config**

### Cách 1: Export JSON:
1. **Vào Dashboard Settings (⚙️)**
2. **Download JSON**
3. **Lưu file vào: `dashboards/superset_config.json`**

### Cách 2: Export ảnh Dashboard:
1. **Screenshot dashboard**
2. **Lưu vào: `dashboards/dashboard_screenshot.png`**

---

## **Bước 7: Push lên GitHub**

```bash
cd D:\PTDLL
git add dashboards/ src/airflow_dag_pipeline.py docker-compose-with-airflow.yml
git commit -m "Add Airflow DAG + Superset Dashboard config"
git push origin main
```

---

## **Troubleshooting**

### Airflow DAG không load:
```bash
docker exec airflow-scheduler airflow dags list
```

### Spark Thrift Server không connect từ Superset:
```bash
docker exec master bash -c "echo 'SELECT 1' | hive"
# Hoặc check port:
docker exec master netstat -tlnp | grep 10001
```

### Reset Airflow:
```bash
docker exec airflow-scheduler rm -f /opt/airflow/airflow.db
docker-compose -f docker-compose-with-airflow.yml restart airflow-scheduler
```

---

## **URL References**

| Service | URL | User/Pass |
|---------|-----|-----------|
| Airflow | http://localhost:8081 | admin/admin |
| Superset | http://localhost:8080 | admin/admin |
| HDFS NameNode | http://localhost:9870 | - |
| YARN | http://localhost:8089 | - |

---

## **Architecture**

```
Data Flow:
  CSV Files (./data/) 
    ↓
  Bronze Layer (01_brz) - Airflow Task 1
    ↓ (Parquet @ /lakehouse/brz/)
  Silver Layer (02_slv) - Airflow TaskGroup (7 tasks)
    ↓ (Parquet @ /lakehouse/slv/)
  Gold Layer (03_gld) - Airflow TaskGroup (2 tasks)
    ↓ (Parquet @ /lakehouse/gld/)
  Superset Dashboard ← Query via Spark Thrift Server
```
