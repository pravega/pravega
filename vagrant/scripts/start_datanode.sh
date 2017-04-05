export JAVA_HOME=/usr/lib/jvm/java-8-oracle
export HADOOP_HOME=/opt/pravega/pravega-release/hadoop/hadoop-2.7.3
$HADOOP_HOME/bin/hdfs datanode &> /tmp/datanode_`hostname`.log &
