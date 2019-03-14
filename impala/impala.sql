
DROP TABLE IF EXISTS crimes;

hdfs dfs -put /home/cloudera/Downloads/Crimes_-_2001_to_present.csv /user/impala/prueba

CREATE TABLE IF NOT EXISTS crimes (
   id INT,
   case_number STRING,
   fecha STRING,
   block STRING,
   iucr STRING,
   primary_type STRING,
   description STRING,
   location_description STRING,
   arrest BOOLEAN,
   domestic BOOLEAN,
   beat INT,
   district INT,
   ward INT,
   community_area STRING,
   fbi_code INT,
   x_coordinate INT,
   y_coordinate INT,
   year INT,
   updated_on STRING,
   latitude STRING,
   longitude STRING,
   localizacion STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED
AS TEXTFILE;

hdfs dfs -put /home/cloudera/Downloads/Crimes_-_2001_to_present.csv /user/impala/prueba
alter table crimes set tblproperties('skip.header.line.count'='1');
LOAD DATA INPATH '/user/impala/prueba/Crimes_-_2001_to_present.csv' OVERWRITE INTO TABLE crimes;

SELECT primary_type, Count(*) as TheCount
FROM
(
  SELECT *
  FROM crimes WHERE year
  BETWEEN 2010 AND 2019
) sub
GROUP BY primary_type;