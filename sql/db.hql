DROP DATABASE IF EXISTS team36_projectdb CASCADE;

CREATE DATABASE team36_projectdb LOCATION "project_table";
USE team36_projectdb;

CREATE EXTERNAL TABLE accidents_hql(
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
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project_table/accidents_hql';

LOAD DATA INPATH '/user/team36/dataset/accidents.csv' OVERWRITE INTO TABLE accidents_hql;

SELECT * FROM accidents_part_buck LIMIT 10;