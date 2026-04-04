"""
AIRFLOW DAG - Pipeline Medallion tự động

Lịch chạy: Hàng ngày lúc 08:00 AM UTC
Dependencies: Bronze → Silver → Gold

Chạy từ container master:
  airflow dags test ptdll_medallion_pipeline 2026-04-04
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

# Cấu hình DAG
default_args = {
    'owner': 'data_eng',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 4, 4),
}

dag = DAG(
    'ptdll_medallion_pipeline',
    default_args=default_args,
    description='Olist Medallion Architecture: Bronze → Silver → Gold',
    schedule_interval='0 8 * * *',  # Mỗi ngày 08:00 UTC
    catchup=False,
    tags=['medallion', 'olist', 'etl'],
)

HDFS = "hdfs://master:9000/lakehouse"
SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
APP_SRC = "/app/src"

# ============================================================================
# BRONZE LAYER
# ============================================================================
task_bronze = BashOperator(
    task_id='bronze_ingest_all',
    bash_command=f"""
    {SPARK_SUBMIT} \
        --master spark://master:7077 \
        --driver-memory 2g \
        --executor-memory 2g \
        --executor-cores 2 \
        {APP_SRC}/01_brz/brz_ingest_all.py
    """,
    dag=dag,
)

# ============================================================================
# SILVER LAYER - 7 files
# ============================================================================
silver_files = [
    'slv_orders',
    'slv_products',
    'slv_customers',
    'slv_sellers',
    'slv_order_items',
    'slv_order_payments',
    'slv_order_reviews',
]

with TaskGroup('silver_layer', dag=dag) as silver_tg:
    silver_tasks = []
    for file_name in silver_files:
        task = BashOperator(
            task_id=file_name,
            bash_command=f"""
            {SPARK_SUBMIT} \
                --master spark://master:7077 \
                --driver-memory 2g \
                --executor-memory 2g \
                --executor-cores 2 \
                {APP_SRC}/02_slv/{file_name}.py
            """,
        )
        silver_tasks.append(task)

# ============================================================================
# GOLD LAYER - 2 files
# ============================================================================
gold_files = [
    'gld_delivery_perf',
    'gld_sales_report',
]

with TaskGroup('gold_layer', dag=dag) as gold_tg:
    gold_tasks = []
    for file_name in gold_files:
        task = BashOperator(
            task_id=file_name,
            bash_command=f"""
            {SPARK_SUBMIT} \
                --master spark://master:7077 \
                --driver-memory 2g \
                --executor-memory 2g \
                --executor-cores 2 \
                {APP_SRC}/03_gld/{file_name}.py
            """,
        )
        gold_tasks.append(task)

# ============================================================================
# NOTIFICATION (tùy chọn)
# ============================================================================
task_success = BashOperator(
    task_id='pipeline_success',
    bash_command='echo "Pipeline Medallion chạy xong! Dữ liệu tại: {{ params.hdfs }}/gld/"',
    params={'hdfs': HDFS},
    dag=dag,
)

# ============================================================================
# DEPENDENCIES
# ============================================================================
task_bronze >> silver_tg >> gold_tg >> task_success
