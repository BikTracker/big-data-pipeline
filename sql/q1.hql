USE team36_projectdb;

DROP TABLE IF EXISTS q1_results;
-- avg severity by state
CREATE EXTERNAL TABLE q1_results(
    state string,
    avg_severity float
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location 'project/hive/warehouse/q1';

-- to not display table names with column names
SET hive.resultset.use.unique.column.names = false;

INSERT INTO q1_results
SELECT state,
       avg(severity) as avg_severity
FROM accidents_part_buck
group by state;

INSERT OVERWRITE DIRECTORY 'project/output/q1'
ROW FORMAT DELIMITED FIELDS
TERMINATED BY ','
SELECT * FROM q1_results;