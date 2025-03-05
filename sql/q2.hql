USE team36_projectdb;

DROP TABLE IF EXISTS q2_results;
-- avg severity by state
CREATE EXTERNAL TABLE q2_results(
    city string,
    accidents_number int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location 'project/hive/warehouse/q2';

-- to not display table names with column names
SET hive.resultset.use.unique.column.names = false;

INSERT INTO q2_results
select city, 
       count(*) as accidents_number
from accidents_part_buck
GROUP BY city
order by count(*) desc
limit 10;

INSERT OVERWRITE DIRECTORY 'project/output/q2'
ROW FORMAT DELIMITED FIELDS
TERMINATED BY ','
SELECT * FROM q2_results;