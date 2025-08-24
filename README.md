#Startup Process
#As hduser !!!

#start HDFS
start-dfs.sh

#Start YARN
start-yarn.sh

#Start Zookeeper
zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties &

#Start Kafka
kafka-server-start.sh $KAFKA_HOME/config/server.properties &




# Command to be run in: (de-venv) student@MSI:~/de-prj$
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1 consumer.py
# Command to be run in: (de-venv) student@MSI:~/de-prj$
python producer.py



#For debug purpose only
#(de-venv) student@MSI:~$ hdfs dfs -ls /user/student/water_quality_raw
hdfs dfs -ls /user/student/water_quality_raw

(de-venv) student@MSI:~$ hdfs dfs -rm -r /user/student/water_quality_raw

hdfs dfs -rm -r /user/student/water_quality_raw
hdfs dfs -rm -r /user/student/checkpoints_raw





