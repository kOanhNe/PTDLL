from prefect import flow, task, get_run_logger
import paramiko

# ==========================================================
# CAU HINH KET NOI (SSH -> MASTER CONTAINER)
# ==========================================================
SSH_HOST = "master"
SSH_USER = "root"
SSH_PASS = "1"
BASE_PATH = "/app/src"

HDFS_BIN = "/opt/hadoop/bin/hdfs"
SPARK_SUBMIT = "/opt/spark/bin/spark-submit"

# ==========================================================
# HAM CHAY LENH QUA SSH
# ==========================================================
def run_remote_cmd(cmd):
    logger = get_run_logger()
    logger.info(f"[SSH] {cmd}")
    
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        client.connect(SSH_HOST, username=SSH_USER, password=SSH_PASS)
        stdin, stdout, stderr = client.exec_command(cmd)

        output = stdout.read().decode()
        error = stderr.read().decode()
        exit_status = stdout.channel.recv_exit_status()

        if exit_status != 0:
            logger.error(error)
            raise Exception(f"Loi khi chay lenh: {cmd}")

        if output:
            logger.info(output)

        return output

    finally:
        client.close()

# ==========================================================
# BUOC 1: LOAD DATA LEN HDFS
# ==========================================================
@task(name="01_Load_Raw_Data", retries=2, retry_delay_seconds=10)
def upload_data_to_hdfs():
    logger = get_run_logger()
    logger.info("Uploading CSV -> HDFS...")

    cmds = [
        f"{HDFS_BIN} dfs -mkdir -p /lakehouse/raw",
        f"{HDFS_BIN} dfs -chmod -R 777 /lakehouse",
        f"{HDFS_BIN} dfs -put -f /data/*.csv /lakehouse/raw/"
    ]

    for cmd in cmds:
        run_remote_cmd(cmd)

# ==========================================================
# BUOC 2: BRONZE
# ==========================================================
@task(name="02_Bronze_Layer")
def run_bronze():
    logger = get_run_logger()
    logger.info("Running Bronze Layer...")

    cmd = f"{SPARK_SUBMIT} {BASE_PATH}/01_brz/brz_ingest_all.py"
    run_remote_cmd(cmd)

# ==========================================================
# BUOC 3: SILVER
# ==========================================================
@task(name="03_Silver_Layer")
def run_silver():
    logger = get_run_logger()
    logger.info("Running Silver Layer...")

    scripts = [
        "slv_orders.py",
        "slv_order_items.py",
        "slv_customers.py",
        "slv_sellers.py",
        "slv_order_payments.py",
        "slv_order_reviews.py",
        "slv_products.py"
    ]

    for script in scripts:
        cmd = f"{SPARK_SUBMIT} {BASE_PATH}/02_slv/{script}"
        run_remote_cmd(cmd)

# ==========================================================
# BUOC 4: GOLD
# ==========================================================
@task(name="04_Gold_Layer")
def run_gold():
    logger = get_run_logger()
    logger.info("Running Gold Layer...")

    scripts = [
        "gld_sales_report.py",
        "gld_delivery_perf.py"
    ]

    for script in scripts:
        cmd = f"{SPARK_SUBMIT} {BASE_PATH}/03_gld/{script}"
        run_remote_cmd(cmd)

# ==========================================================
# BUOC 5: VALIDATE
# ==========================================================
@task(name="05_Validate_Output")
def validate_lakehouse():
    logger = get_run_logger()
    logger.info("Checking Gold data in HDFS...")

    run_remote_cmd(f"{HDFS_BIN} dfs -ls /lakehouse/gld/")

# ==========================================================
# FLOW CHINH
# ==========================================================
@flow(name="Olist_Lakehouse_ETL_Flow")
def etl_lakehouse_flow():

    upload_data_to_hdfs()
    run_bronze()
    run_silver()
    run_gold()
    validate_lakehouse()

    logger = get_run_logger()
    logger.info("======================================")
    logger.info("ETL PIPELINE HOAN TAT!")
    logger.info("Mo Superset: http://localhost:8080")
    logger.info("======================================")

# ==========================================================
if __name__ == "__main__":
    etl_lakehouse_flow()
