FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# 1. Cài đặt các gói cơ bản, Java 17 và OPENSSH-SERVER
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    openssh-server \
    ssh \
    pdsh \
    vim \
    wget \
    net-tools \
    iputils-ping \
    && apt-get clean

# --- CẤU HÌNH SSH ĐỂ PREFECT CÓ THỂ KẾT NỐI QUA NETWORK ---
RUN mkdir /var/run/sshd
# Đặt password cho root là '1' để Prefect login qua SSH
RUN echo 'root:1' | chpasswd
# Cho phép root login và xác thực bằng password
RUN sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config
RUN sed -i 's/session    required     pam_loginuid.so/session    optional     pam_loginuid.so/g' /etc/pam.d/sshd

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# 2. Cài đặt Hadoop 3.3.6
ADD hadoop-3.3.6.tar.gz /opt/
RUN mv /opt/hadoop-3.3.6 /opt/hadoop
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# 3. Cài đặt Hive 2.3.10
ADD apache-hive-2.3.10-bin.tar.gz /opt/
RUN mv /opt/apache-hive-2.3.10-bin /opt/hive
ENV HIVE_HOME=/opt/hive
ENV PATH=$PATH:$HIVE_HOME/bin

# 4. Cài đặt Spark 4.1.1
ADD spark-4.1.1-bin-hadoop3.tgz /opt/
RUN mv /opt/spark-4.1.1-bin-hadoop3 /opt/spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# 5. FIX LỖI THƯ VIỆN & PHÂN QUYỀN SSH NỘI BỘ (HADOOP)
RUN rm -f $HIVE_HOME/lib/guava-*.jar && \
    cp $HADOOP_HOME/share/hadoop/common/lib/guava-*.jar $HIVE_HOME/lib/

# SSH nội bộ cho Hadoop cluster tự nói chuyện với nhau
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 0600 ~/.ssh/authorized_keys && \
    echo "StrictHostKeyChecking no" >> /etc/ssh/ssh_config

# 6. THIẾT LẬP CẤU HÌNH
COPY docker/config/* $HADOOP_CONF_DIR/
RUN find $HADOOP_CONF_DIR -type f -exec sed -i 's/\r$//' {} +

COPY docker/config/hive-site.xml $HIVE_HOME/conf/
COPY docker/config/hive-site.xml $SPARK_HOME/conf/
COPY docker/config/spark-defaults.conf $SPARK_HOME/conf/
RUN sed -i 's/\r$//' $SPARK_HOME/conf/spark-defaults.conf

# 7. BIẾN MÔI TRƯỜNG HADOOP USER
ENV HDFS_NAMENODE_USER=root
ENV HDFS_DATANODE_USER=root
ENV HDFS_SECONDARYNAMENODE_USER=root
ENV YARN_RESOURCEMANAGER_USER=root
ENV YARN_NODEMANAGER_USER=root

# 8. KHỞI CHẠY
COPY docker/start.sh /start.sh
RUN sed -i 's/\r$//' /start.sh && chmod +x /start.sh

WORKDIR /opt/hadoop
# Khi container chạy, nó sẽ thực thi file start.sh
ENTRYPOINT ["/start.sh"]