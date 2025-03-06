USE team36_projectdb;

-- to not display table names with column names
SET hive.resultset.use.unique.column.names = false;

DROP TABLE IF EXISTS model1_predictions;

CREATE EXTERNAL TABLE model1_predictions(
    label int,
    prediction float
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location 'project/hive/warehouse/model1_predictions'
tblproperties("skip.header.line.count"="1");

LOAD DATA INPATH '/user/team36/output/model1_predictions.csv' OVERWRITE INTO TABLE model1_predictions;

select * from model1_predictions limit 5;