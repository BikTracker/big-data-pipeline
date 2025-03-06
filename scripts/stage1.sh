#!/bin/bash
url="https://disk.yandex.ru/d/WfsUS2K8esyCrA"

rm -f data/accidents.csv

wget "$(yadisk-direct "${url}")" -O data/accidents.csv

printf "Started preprocessing\n"
python3 scripts/preprocess.py
printf "Finished \n \n"

printf "Started building database\n"
python3 scripts/build_db.py
printf "Finished\n"

password=$(head -n 1 secrets/.psql.pass)

hdfs dfs -rm -R -skipTrash project/warehouse/*

sqoop import-all-tables --connect jdbc:postgresql://hadoop-04.uni.innopolis.ru/team36_projectdb --username team36 --password $password --compression-codec=snappy --compress --as-avrodatafile --warehouse-dir=project/warehouse --m 1

mv accidents.java output/
mv accidents.avsc output/
