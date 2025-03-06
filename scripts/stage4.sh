#!/bin/bash

password=$(head -n 1 secrets/.hive.pass)

python3 scripts/fix_format.py

hdfs dfs -rm -f -R -skipTrash project/output/model1_predictions.csv
hdfs dfs -rm -f -R -skipTrash project/output/model2_predictions.csv
hdfs dfs -rm -f -R -skipTrash project/output/evaluation.csv

# put data into hdfs
hdfs dfs -put output/model1_predictions.csv project/output
hdfs dfs -put output/model2_predictions.csv project/output
hdfs dfs -put output/evaluation.csv project/output

# model1
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team36 -p $password -f sql/model1_predictions.hql
# model2
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team36 -p $password -f sql/model2_predictions.hql
# evaluation
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team36 -p $password -f sql/evaluation.hql
