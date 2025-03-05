DROP DATABASE IF EXISTS team36_projectdb CASCADE;

CREATE DATABASE team36_projectdb LOCATION "project/hive/warehouse";
USE team36_projectdb;

CREATE EXTERNAL TABLE accidents STORED AS AVRO LOCATION 'project/warehouse/accidents' TBLPROPERTIES ('avro.schema.url'='project/warehouse/avsc/accidents.avsc');

select * from accidents limit 10;

CREATE EXTERNAL TABLE accidents_part_buck (
    ID varchar(10),
    Source varchar(10),
    Severity smallint,
    Start_Time string,
    End_Time string,
    Start_Lat float,
    Start_Lng float,
    End_Lat float,
    End_Lng float,
    Distance float,
    Description string,
    Street string,
    City string,
    County string,
    Zipcode string,
    Country string,
    Timezone string,
    Airport_Code string,
    Weather_Timestamp string,
    Temperature float,
    Wind_Chill float,
    Humidity float,
    Pressure float,
    Visibility float,
    Wind_Direction string,
    Wind_speed float,
    Precipitation float,
    Weather_Condition string,
    Amenity boolean,
    Bump boolean,
    Crossing boolean,
    Give_Way boolean,
    Junction boolean,
    No_Exit boolean,
    Railway boolean,
    Roundabout boolean,
    Station boolean,
    Stop boolean,
    Traffic_Calming boolean,
    Traffic_Signal boolean,
    Turning_Loop boolean,
    Sunrise_Sunset string,
    Civil_Twilight string,
    Nautical_Twilight string,
    Astronomical_Twilight string
)
PARTITIONED BY (State string)
CLUSTERED BY (Severity) into 2 buckets
STORED AS AVRO LOCATION 'project/hive/warehouse/accidents_part_buck'
TBLPROPERTIES ('AVRO.COMPRESS'='SNAPPY');

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE accidents_part_buck 
SELECT ID, Source, Severity, Start_Time, End_Time, Start_Lat, Start_Lng, End_Lat, End_Lng,
        Distance, Description, Street, City, County, Zipcode, Country, Timezone, 
        Airport_Code, Weather_Timestamp, Temperature, Wind_Chill, Humidity, Pressure, Visibility,
        Wind_Direction, Wind_speed, Precipitation, Weather_Condition, Amenity, Bump, Crossing,
        Give_Way, Junction, No_Exit, Railway, Roundabout, Station, Stop, 
        Traffic_Calming, Traffic_Signal, Turning_Loop, Sunrise_Sunset, 
        Civil_Twilight, Nautical_Twilight, Astronomical_Twilight, State
FROM accidents;

drop table accidents;

SELECT * FROM accidents_part_buck LIMIT 10;
show partitions accidents_part_buck;