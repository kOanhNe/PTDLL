FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# 1. Cài đặt các gói cơ bản và Java 21
RUN apt-get update && apt-get install -y \
    openjdk-21-jdk \
    ssh \
    pdsh \
    vim \
    wget \
    net-tools \
    && apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# 2. Cài đặt Hadoop 3.3.6 (Dùng ADD để tự giải nén tiết kiệm chỗ)
ADD hadoop-3.3.6.tar.gz /opt/
RUN mv /opt/hadoop-3.3.6 /opt/hadoop
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# --- BỎ PHẦN PIG TẠI ĐÂY ---

# 3. Cài đặt Hive 4.2.0
ADD apache-hive-4.2.0-bin.tar.gz /opt/
RUN mv /opt/apache-hive-4.2.0-bin /opt/hive
ENV HIVE_HOME=/opt/hive
ENV PATH=$PATH:$HIVE_HOME/bin

# 4. Cài đặt Spark 4.1.1
ADD spark-4.1.1-bin-hadoop3.tgz /opt/
RUN mv /opt/spark-4.1.1-bin-hadoop3 /opt/spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# 5. FIX LỖI THƯ VIỆN & PHÂN QUYỀN SSH
# Đồng bộ Guava giữa Hadoop và Hive để tránh lỗi version
RUN rm -f $HIVE_HOME/lib/guava-*.jar && \
    cp $HADOOP_HOME/share/hadoop/common/lib/guava-*.jar $HIVE_HOME/lib/

RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 0600 ~/.ssh/authorized_keys && \
    echo "StrictHostKeyChecking no" >> /etc/ssh/ssh_config

# 6. THIẾT LẬP CẤU HÌNH
COPY docker/config/* $HADOOP_CONF_DIR/
COPY docker/config/hive-site.xml $HIVE_HOME/conf/
# Mở comment dòng này nếu bạn muốn Spark nhận cấu hình Hive ngay lập tức
COPY docker/config/hive-site.xml $SPARK_HOME/conf/

# 7. BIẾN MÔI TRƯỜNG HADOOP USER
ENV HDFS_NAMENODE_USER=root
ENV HDFS_DATANODE_USER=root
ENV HDFS_SECONDARYNAMENODE_USER=root
ENV YARN_RESOURCEMANAGER_USER=root
ENV YARN_NODEMANAGER_USER=root

# 8. KHỞI CHẠY
COPY docker/start.sh /start.sh
RUN chmod +x /start.sh

WORKDIR /opt/hadoop
ENTRYPOINT ["/start.sh"]