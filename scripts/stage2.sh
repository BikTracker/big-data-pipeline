#!/bin/bash

hdfs dfs -mkdir -p project/warehouse/avsc
hdfs dfs -put output/accidents.avsc project/warehouse/avsc

password=$(head -n 1 secrets/.hive.pass)

beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team36 -p $password -f sql/db.hql > output/hive_results.txt 
# 2> /dev/null

# q1
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team36 -p $password -f sql/q1.hql
echo "state,avg_severity" > output/q1.csv 
hdfs dfs -cat project/output/q1/* >> output/q1.csv

# q2
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team36 -p $password -f sql/q2.hql
echo "city,accidents_number" > output/q2.csv 
hdfs dfs -cat project/output/q2/* >> output/q2.csv

# q3
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team36 -p $password -f sql/q3.hql
echo "country,county,city,accidents_number" > output/q3.csv 
hdfs dfs -cat project/output/q3/* >> output/q3.csv

# q4
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team36 -p $password -f sql/q4.hql
echo "state,num_of_cities,num_of_accidents,num_of_accidents_per_city" > output/q4.csv 
hdfs dfs -cat project/output/q4/* >> output/q4.csv

# q5
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team36 -p $password -f sql/q5.hql
echo "state,avg_severity,avg_humidity,avg_temperature,avg_pressure,avg_visibility,avg_wind_speed" > output/q5.csv
hdfs dfs -cat project/output/q5/* >> output/q5.csv