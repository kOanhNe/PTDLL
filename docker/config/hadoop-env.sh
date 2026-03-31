#!/bin/bash

export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

# Cho phép Hadoop chạy bằng root (trong Docker)
export HDFS_NAMENODE_USER=root
export HDFS_DATANODE_USER=root
export HDFS_SECONDARYNAMENODE_USER=root

export YARN_RESOURCEMANAGER_USER=root
export YARN_NODEMANAGER_USER=root
export PDSH_RCMD_TYPE=ssh

# MapReduce & YARN runtime
export HADOOP_HOME=/opt/hadoop
export HADOOP_MAPRED_HOME=/opt/hadoop
export YARN_HOME=/opt/hadoop
export HIVE_HOME=/opt/hive
export PATH=$PATH:$HIVE_HOME/bin

export HADOOP_OPTS="$HADOOP_OPTS --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED"

export YARN_OPTS="$YARN_OPTS --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED"