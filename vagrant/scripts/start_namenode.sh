export JAVA_HOME=/usr/lib/jvm/java-8-oracle
export HADOOP_HOME=/opt/pravega/pravega-release/hadoop/hadoop-2.7.3
$HADOOP_HOME/bin/hdfs namenode -format &> /tmp/namenode_format_`hostname`.log
$HADOOP_HOME/bin/hdfs namenode &> /tmp/namenode_`hostname`.log &
