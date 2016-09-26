drop table drivers_IL_2016_08;

set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
set mapred.reduce.tasks = 1;
--SET hive.exec.compress.intermediate=true;
--SET hive.exec.compress.output=true;
--SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
SET hive.cli.print.header=true;

CREATE TABLE `drivers_IL_2016_08` (
  driver_gk int,
  country_symbol varchar(2),
  city varchar(30),
  postal_address varchar(100),
  taxi_type_id int,
  taxi_type_desc varchar(10),
  driver_computed_rating int,
  number_of_rates int,
  number_of_orders int,
  num_days_online int,
  number_days_getting int,
  number_of_rides int,
  number_of_rejected_orders int,
  total_number_of_rejects_and_ignore int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar"     = "\"",
"escapeChar"    = "\\"
)
STORED AS TEXTFILE LOCATION 's3://presto-db/gett_algo/datasets/driver_offers/drivers_IL_2016_08/'


insert overwrite table drivers_IL_2016_08
SELECT
      driver_gk,
      country_symbol,
      city,
      postal_address,
      taxi_type_id,
      taxi_type_desc,
      driver_computed_rating,
      number_of_rates,
      number_of_orders,
      num_days_online,
      number_days_getting,
      number_of_rides,
      number_of_rejected_orders,
      total_number_of_rejects_and_ignore
from dwh.dim_drivers_v
where driver_gk in (
    SELECT
    distinct offers.driver_gk
  FROM dwh.fact_offers_v AS offers
  WHERE offers.date_key >= DATE '2016-08-01' AND offers.date_key < DATE '2016-09-01'
        AND offers.offer_status_key <> 6
        AND offers.country_symbol = 'IL'
)



-- offer statuses:
-------------------
--2	Accepted
--5	Cancelled
--4	Confirmed
--1	Pending
--3	Rejected
--6	Withdrawn


-- python & hive:
>>> from pyhive import hive
>>> cursor = hive.connect('localhost').cursor()
>>> cursor.execute("show databases")
>>> print cursor.fetchall()
[(u'analyst',), (u'data_fix',), (u'default',), (u'dwh',), (u'gett_algo',), (u'peta_locs',), (u'test',)]
>>> cursor.execute("use gett_algo")
>>> cursor.execute("show tables")
>>> print cursor.fetchall()
[(u'driver_offers_il_2016_08',)]


presto-cli --catalog hive --schema gett_algo --execute "select concat('aws s3 cp --recursive s3://presto-db/gett_algo/datasets/driver_offers/driver_offers_IL_2016_08/driver_id=',driver_id,'/ /mnt1/gett_algo/datasets/driver_offers/driver_offers_IL_2016_08/',driver_id) from (select count(*), cast(driver_id as varchar) driver_id from driver_offers_IL_2016_08 group by 2 having count(*) >= 1000)" > download_files.sh; sed -i 's/"//g' download_files.sh; chmod +x download_files.sh



