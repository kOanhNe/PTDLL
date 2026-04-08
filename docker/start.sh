#!/bin/bash
set -e

# 1. Khởi động SSH
service ssh start
sleep 5

if [ "$HOSTNAME" = "master" ]; then
    # Nạp biến môi trường Hadoop & Spark
    source /opt/hadoop/etc/hadoop/hadoop-env.sh
    export SPARK_HOME=/opt/spark  # Đảm bảo đường dẫn này đúng với folder Spark của bạn
    export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

    # 2. Format Namenode nếu chưa có
    if [ ! -d "/tmp/hadoop-root/dfs/name" ]; then
        echo "Formatting NameNode..."
        /opt/hadoop/bin/hdfs namenode -format
    fi

    # 3. Khởi động HDFS & YARN
    echo "Starting HDFS & YARN..."
    /opt/hadoop/sbin/start-dfs.sh
    /opt/hadoop/sbin/start-yarn.sh

    # 4. Đợi HDFS sẵn sàng (Thoát Safe Mode)
    echo "Waiting for HDFS to leave safe mode..."
    /opt/hadoop/bin/hdfs dfsadmin -safemode wait

    # 5. Tạo thư mục hệ thống cho Lakehouse
    echo "Preparing HDFS folders..."
    /opt/hadoop/bin/hadoop fs -mkdir -p /user/hive/warehouse
    /opt/hadoop/bin/hadoop fs -chmod g+w /user/hive/warehouse
    /opt/hadoop/bin/hadoop fs -mkdir -p /tmp
    /opt/hadoop/bin/hadoop fs -chmod g+w /tmp

    # --- CẤU HÌNH CHO LAKEHOUSE & SUPERSET ---

    wait_for_port() {
        local host="$1"
        local port="$2"
        local retries="${3:-30}"
        local sleep_sec="${4:-2}"

        for i in $(seq 1 "$retries"); do
            if bash -c "</dev/tcp/${host}/${port}" >/dev/null 2>&1; then
                echo "Port ${host}:${port} is ready"
                return 0
            fi
            echo "Waiting ${host}:${port} (${i}/${retries})..."
            sleep "$sleep_sec"
        done

        echo "ERROR: ${host}:${port} is not ready after ${retries} retries"
        return 1
    }

    # 6. Khởi tạo schema + khởi động Hive Metastore (bắt buộc cho Hive 4)
    echo "Checking Hive Metastore schema..."
    if ! schematool -dbType derby -info >/opt/hadoop/logs/hive-schematool.log 2>&1; then
        echo "Initializing Hive Metastore schema..."
        schematool -dbType derby -initSchema >> /opt/hadoop/logs/hive-schematool.log 2>&1
    fi

    echo "Starting Hive Metastore..."
    nohup hive --service metastore > /opt/hadoop/logs/hive-metastore.log 2>&1 &
    wait_for_port localhost 9083 40 2

    # 7. Khởi động Spark Thrift Server (Cổng kết nối SQL cho Superset)
    # Dùng cổng 10001 để tránh đụng độ với Hive (nếu có)
    echo "Starting Spark Thrift Server (The Lakehouse Engine)..."
    $SPARK_HOME/sbin/start-thriftserver.sh \
        --master yarn \
        --deploy-mode client \
        --conf spark.hadoop.hive.metastore.uris=thrift://master:9083 \
        --conf spark.sql.warehouse.dir=/user/hive/warehouse \
        --conf spark.executor.memory=1g \
        --conf spark.driver.memory=1g \
        --hiveconf hive.server2.thrift.bind.host=0.0.0.0 \
        --hiveconf hive.server2.thrift.port=10001 \
        --hiveconf hive.server2.authentication=NOSASL

    wait_for_port localhost 10001 50 2
    
    echo "Lakehouse services are ready!"
fi

# Giữ container không bị thoát
tail -f /dev/null