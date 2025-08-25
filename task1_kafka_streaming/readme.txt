hdfs dfs -rm -r /user/student/water_quality_raw
hdfs dfs -rm -r /user/student/checkpoints_raw

# Command to be run in: (de-venv) student@MSI:~/de-prj$
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1 consumer.py

# Command to be run in: (de-venv) student@MSI:~/de-prj$
python producer.py

hdfs dfs -ls /user/student/water_quality_raw
