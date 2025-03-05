USE team36_projectdb;

-- to not display table names with column names
SET hive.resultset.use.unique.column.names = false;

DROP TABLE IF EXISTS evaluation;

CREATE EXTERNAL TABLE evaluation(
    model string,
    acc float,
    f1 float
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location 'project/hive/warehouse/evaluation'
tblproperties("skip.header.line.count"="1");

LOAD DATA INPATH '/user/team36/project/output/evaluation.csv' OVERWRITE INTO TABLE evaluation;

select * from evaluation limit 5;