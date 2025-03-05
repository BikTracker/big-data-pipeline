USE team36_projectdb;

DROP TABLE IF EXISTS q4_results;
-- avg severity by state
CREATE EXTERNAL TABLE q4_results(
    state string,
    num_of_cities int,
    num_of_accidents int,
    num_of_accidents_per_city float
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location 'project/hive/warehouse/q4';

-- to not display table names with column names
SET hive.resultset.use.unique.column.names = false;

INSERT INTO q4_results
select state, num_of_cities, num_of_accidents, round((num_of_accidents / num_of_cities), 2) as num_of_accidents_per_city 
from (select state, 
        count(distinct city) as num_of_cities, 
        count(*) as num_of_accidents
    from accidents_part_buck
    GROUP BY state
    order by count(*) desc
    limit 10) a;

INSERT OVERWRITE DIRECTORY 'project/output/q4'
ROW FORMAT DELIMITED FIELDS
TERMINATED BY ','
SELECT * FROM q4_results;