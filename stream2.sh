export TEZ_CONF_DIR="/opt/mapr/tez/tez-0.8/conf"
export TEZ_JARS="/opt/mapr/tez/tez-0.8/*:/opt/mapr/tez/tez-0.8/lib/*"
export HADOOP_CLASSPATH="$TEZ_CONF_DIR:$TEZ_JARS:$HADOOP_CLASSPATH"
export MAPR_TICKETFILE_LOCATION=/tmp/maprticket_9003
export HADOOP_CONF_DIR=/opt/mapr/hadoop/hadoop-2.7.0/etc/hadoop
export HIVE_CONF_DIR=/opt/mapr/hive/hive-2.1/conf
MASTER=yarn-client /opt/mapr/spark/spark-2.2.1/bin/spark-submit --executor-memory 2G --num-executors 20 --class azureSensortag /path-to-jar/consume-iot-azure-by-spark-classic-streaming_2.11-0.0.1.jar "broker1.xxxxx.com:9092,broker2.xxxxx.com:9092,broker3.xxxxx.com:9092" "/dds-iot:events" "$Default" latest 1 2000 L 5000 1000
