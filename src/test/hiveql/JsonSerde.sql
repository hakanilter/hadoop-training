DROP TABLE IF EXISTS NasaJson;
CREATE EXTERNAL TABLE NasaJson (
  `id` STRING,
  mass STRING,
  `name` STRING,
  nametype STRING,
  fall STRING,
  geolocation STRUCT<type: STRING, coordinates: ARRAY<DOUBLE>>,
  recclass STRING,
  reclat STRING,
  reclong STRING,
  `year` STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION '/user/hadoop/results/json';

SELECT * FROM NasaJson;