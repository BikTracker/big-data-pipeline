USE team36_projectdb;

DROP TABLE IF EXISTS q3_results;
-- avg severity by state
CREATE EXTERNAL TABLE q3_results(
    country string,
    county string,
    city string,
    accidents_number int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location 'project/hive/warehouse/q3';

-- to not display table names with column names
SET hive.resultset.use.unique.column.names = false;

INSERT INTO q3_results
select country, 
       county, 
       city, 
       count(*) as accidents_number
from accidents_part_buck
GROUP BY country, county, city
ORDER BY count(*) desc
limit 1;

INSERT OVERWRITE DIRECTORY 'project/output/q3'
ROW FORMAT DELIMITED FIELDS
TERMINATED BY ','
SELECT * FROM q3_results;