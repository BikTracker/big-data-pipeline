USE team36_projectdb;

-- to not display table names with column names
SET hive.resultset.use.unique.column.names = false;

DROP TABLE IF EXISTS model2_predictions;

CREATE EXTERNAL TABLE model2_predictions(
    label int,
    prediction float
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location 'project/hive/warehouse/model2_predictions'
tblproperties("skip.header.line.count"="1");

LOAD DATA INPATH '/user/team36/project/output/model2_predictions.csv' OVERWRITE INTO TABLE model2_predictions;

select * from model2_predictions limit 5;