#!/bin/bash
set -e

# 1. Khởi động SSH 
service ssh start
# Tạo thư mục logs nếu chưa có để tránh lỗi ghi file
mkdir -p /opt/hadoop/logs

if [ "$HOSTNAME" = "master" ]; then
    # Nạp biến môi trường để các lệnh hdfs, hive, spark nhận diện được
    export HADOOP_HOME=/opt/hadoop
    export HIVE_HOME=/opt/hive
    export SPARK_HOME=/opt/spark
    export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$HIVE_HOME/bin:$SPARK_HOME/bin:$SPARK_HOME/sbin

    # 2. Format Namenode nếu chưa có dữ liệu
    if [ ! -d "/tmp/hadoop-root/dfs/name" ]; then
        echo "Formatting NameNode..."
        $HADOOP_HOME/bin/hdfs namenode -format
    fi

    # 3. Khởi động HDFS & YARN
    echo "Starting HDFS & YARN..."
    $HADOOP_HOME/sbin/start-dfs.sh
    $HADOOP_HOME/sbin/start-yarn.sh

    # 4. Đợi HDFS thoát Safe Mode 
    echo "Waiting for HDFS to leave safe mode..."
    $HADOOP_HOME/bin/hdfs dfsadmin -safemode wait

    # 5. Tạo thư mục hệ thống cho Hive/Lakehouse
    echo "Preparing HDFS folders..."
    $HADOOP_HOME/bin/hadoop fs -mkdir -p /user/hive/warehouse
    $HADOOP_HOME/bin/hadoop fs -chmod g+w /user/hive/warehouse
    $HADOOP_HOME/bin/hadoop fs -mkdir -p /tmp
    $HADOOP_HOME/bin/hadoop fs -chmod g+w /tmp

    # Hàm kiểm tra Port sẵn sàng
    wait_for_port() {
        local host="$1" port="$2"
        while ! timeout 1 bash -c "echo > /dev/tcp/$host/$port" 2>/dev/null; do
            echo "Waiting for $host:$port..."
            sleep 2
        done
    }

    # 6. Khởi tạo Hive Metastore (Dùng Derby làm DB backend)
    echo "Checking Hive Metastore schema..."
    if [ ! -f "/opt/hive/metastore_db/service.properties" ]; then
        echo "Initializing Hive Metastore schema..."
        $HIVE_HOME/bin/schematool -dbType derby -initSchema
    fi

    echo "Starting Hive Metastore..."
    nohup $HIVE_HOME/bin/hive --service metastore > /opt/hadoop/logs/hive-metastore.log 2>&1 &
    wait_for_port localhost 9083

    # 7. Khởi động Spark Thrift Server (Cổng 10001 cho Superset)
    echo "Starting Spark Thrift Server..."
    $SPARK_HOME/sbin/start-thriftserver.sh \
        --master yarn \
        --deploy-mode client \
        --conf spark.hadoop.hive.metastore.uris=thrift://master:9083 \
        --conf spark.sql.warehouse.dir=hdfs://master:9000/user/hive/warehouse \
        --hiveconf hive.server2.thrift.bind.host=0.0.0.0 \
        --hiveconf hive.server2.thrift.port=10001 \
        --hiveconf hive.server2.authentication=NOSASL

    wait_for_port localhost 10001
    echo "Lakehouse services are ready!"
fi

# Giữ container sống
tail -f /dev/null