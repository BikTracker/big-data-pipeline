USE team36_projectdb;

DROP TABLE IF EXISTS q5_results;
-- avg severity by state
CREATE EXTERNAL TABLE q5_results(
    state string,
    avg_severity float,
    avg_humidity float,
    avg_temperature float,
    avg_pressure float,
    avg_visibility float,
    avg_wind_speed float
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
location 'project/hive/warehouse/q5';

-- to not display table names with column names
SET hive.resultset.use.unique.column.names = false;

INSERT INTO q5_results
select state, 
       avg(severity) as avg_severity,
       avg(humidity) as avg_humidity,
       avg(temperature) as avg_temperature,
       avg(pressure) as avg_pressure,
       avg(visibility) as avg_visibility,
       avg(wind_speed) as avg_wind_speed
from accidents_part_buck
group by state;

INSERT OVERWRITE DIRECTORY 'project/output/q5'
ROW FORMAT DELIMITED FIELDS
TERMINATED BY ','
SELECT * FROM q5_results;
