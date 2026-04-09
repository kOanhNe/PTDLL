from prefect import flow, task, get_run_logger
import paramiko
import os

# ==========================================================
# CAU HINH KET NOI (GIAO TIEP QUA MANG)
# ==========================================================
SSH_HOST = "master"
SSH_USER = "root"
SSH_PASS = "1"
BASE_PATH = "/app/src"  # Thu muc code trong container master

# DUONG DAN LENH HE THONG (Dung duong dan tuyet doi)
HDFS_BIN = "/opt/hadoop/bin/hdfs"
SPARK_SUBMIT = "/opt/spark/bin/spark-submit"

# ==========================================================
# HAM HO TRO: CHAY LENH TU XA QUA SSH
# ==========================================================
def run_remote_cmd(cmd):
    logger = get_run_logger()
    logger.info(f"Dang gui lenh: {cmd}")
    
    # Khoi tao ket noi SSH
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        client.connect(SSH_HOST, username=SSH_USER, password=SSH_PASS)
        # Thuc thi lenh tren may chu Master
        stdin, stdout, stderr = client.exec_command(cmd)
        
        # Doc ket qua tra ve
        output = stdout.read().decode('utf-8')
        error = stderr.read().decode('utf-8')
        
        # Kiem tra trang thai thoat (0 la thanh cong)
        exit_status = stdout.channel.recv_exit_status()
        
        if exit_status != 0:
            logger.error(f"Lenh that bai voi ma loi: {exit_status}")
            logger.error(f"Noi dung loi: {error}")
            raise Exception(f"Loi lenh SSH: {cmd}")
            
        if output:
            logger.info(f"Ket qua STDOUT:\n{output}")
            
    finally:
        client.close()

# ==========================================================
# BUOC 1: NAP DU LIEU (Day file CSV tho len HDFS)
# ==========================================================
@task(name="Buoc_1_Nap_Du_Lieu_Tho", retries=2, retry_delay_seconds=15)
def upload_data_to_hdfs():
    logger = get_run_logger()
    logger.info("Bat dau nap du lieu: Dang tai cac file CSV len HDFS...")
    
    cmds = [
        f"{HDFS_BIN} dfs -mkdir -p /lakehouse/raw",
        f"{HDFS_BIN} dfs -chmod -R 777 /lakehouse",
        f"{HDFS_BIN} dfs -put -f /data/*.csv /lakehouse/raw/"
    ]
    
    for cmd in cmds:
        run_remote_cmd(cmd)

# ==========================================================
# BUOC 2: TANG BRONZE (Chuyen CSV sang dinh dang Parquet)
# ==========================================================
@task(name="Buoc_2_Xu_Ly_Tang_Bronze")
def run_bronze():
    logger = get_run_logger()
    logger.info("Dang xu ly tang Bronze...")
    
    # Chay script xu ly Bronze
    spark_cmd = f"{SPARK_SUBMIT} {BASE_PATH}/01_brz/brz_ingest_all.py"
    run_remote_cmd(spark_cmd)

# ==========================================================
# BUOC 3: TANG SILVER (Lam sach va Chuan hoa du lieu)
# ==========================================================
@task(name="Buoc_3_Xu_Ly_Tang_Silver")
def run_silver():
    logger = get_run_logger()
    logger.info("Dang xu ly tang Silver...")
    
    scripts = [
        "slv_orders.py", "slv_order_items.py", "slv_customers.py",
        "slv_sellers.py", "slv_order_payments.py", "slv_order_reviews.py", "slv_products.py"
    ]
    
    for script in scripts:
        logger.info(f"Dang chay script Silver: {script}")
        spark_cmd = f"{SPARK_SUBMIT} {BASE_PATH}/02_slv/{script}"
        run_remote_cmd(spark_cmd)

# ==========================================================
# BUOC 4: TANG GOLD (Tong hop du lieu cho Dashboard)
# ==========================================================
@task(name="Buoc_4_Xu_Ly_Tang_Gold")
def run_gold():
    logger = get_run_logger()
    logger.info("Dang xu ly tang Gold...")
    
    scripts = ["gld_sales_report.py", "gld_delivery_perf.py"]
    
    for script in scripts:
        logger.info(f"Dang chay script Gold: {script}")
        spark_cmd = f"{SPARK_SUBMIT} {BASE_PATH}/03_gld/{script}"
        run_remote_cmd(spark_cmd)

# ==========================================================
# BUOC 5: KIEM TRA CUI CUNG (Xac nhan du lieu)
# ==========================================================
@task(name="Buoc_5_Kiem_Tra_Du_Lieu")
def validate_lakehouse():
    logger = get_run_logger()
    logger.info("Hoan tat: Kiem tra cac bang du lieu tang Gold tren HDFS...")
    # Da sua duong dan thanh /gld/ cho dung voi thuc te HDFS cua be
    run_remote_cmd(f"{HDFS_BIN} dfs -ls /lakehouse/gld/")

# ==========================================================
# LUONG DIEU PHOI CHINH (ORCHESTRATION)
# ==========================================================
@flow(name="Luong_Xu_Ly_Olist_Lakehouse_Toan_Dien")
def etl_lakehouse_flow():
    # Thuc hien tuan tu cac buoc
    upload_data_to_hdfs()
    run_bronze()
    run_silver()
    run_gold()
    validate_lakehouse()

if __name__ == "__main__":
    etl_lakehouse_flow()