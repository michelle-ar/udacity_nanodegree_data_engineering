CREATE EXTERNAL TABLE IF NOT EXISTS `stedi`.`customer_landing` (
  `customerName` string,
  `email` string,
  `phone` string,
  `birthDay` date,
  `serialNumber` string,
  `registrationDate` timestamp,
  `lastUpdateDate` timestamp,
  `shareWithResearchAsOfDate` timestamp,
  `shareWithPublicAsOfDate` timestamp,
  `shareWithFriendsAsOfDate` timestamp
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://project-bucket-udacity-mar/customer_landing/'
TBLPROPERTIES ('classification' = 'json');