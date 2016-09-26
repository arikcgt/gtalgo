drop table driver_offers_IL_2016_08;

set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;
SET hive.exec.compress.intermediate=true;
SET hive.exec.compress.output=true;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;

CREATE TABLE `driver_offers_IL_2016_08` (
    date_key date,
    hour_key date,
    is_accepted int,
    offer_status_key int,
    country_symbol varchar(2),
    distance_from_order_on_creation double,
    ride_type_key int,
    class_type_gk int,
    payment_type_key int
)
PARTITIONED BY (`driver_id` int) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar"     = "\"",
"escapeChar"    = "\\"
)
STORED AS TEXTFILE LOCATION 's3://presto-db/gett_algo/datasets/driver_offers/driver_offers_IL_2016_08/'


insert into driver_offers_IL_2016_08 partition (driver_id)
SELECT
    offers.date_key,
    offers.hour_key,
    offers.is_accepted,
    offers.offer_status_key,
    offers.country_symbol,
    offers.distance_from_order_on_creation,
    orders.ride_type_key,
    orders.class_type_gk,
    orders.payment_type_key,
    offers.driver_gk as driver_id
  FROM dwh.fact_offers_v AS offers
    JOIN dwh.fact_orders_v AS orders
      ON offers.order_gk = orders.order_gk
  WHERE offers.date_key >= '2016-08-01' AND offers.date_key < '2016-09-01'
        AND orders.date_key >= '2016-08-01' AND orders.date_key < '2016-09-01'
        AND offers.offer_status_key <> 6
        AND offers.country_symbol = 'IL'



-- offer statuses:
-------------------
--2	Accepted
--5	Cancelled
--4	Confirmed
--1	Pending
--3	Rejected
--6	Withdrawn


-- python & hive:
-->>> from pyhive import hive
-->>> cursor = hive.connect('localhost').cursor()
-->>> cursor.execute("show databases")
-->>> print cursor.fetchall()
--[(u'analyst',), (u'data_fix',), (u'default',), (u'dwh',), (u'gett_algo',), (u'peta_locs',), (u'test',)]
-->>> cursor.execute("use gett_algo")
-->>> cursor.execute("show tables")
-->>> print cursor.fetchall()
--[(u'driver_offers_il_2016_08',)]


--presto-cli --catalog hive --schema gett_algo --execute "select concat('aws s3 cp --recursive s3://presto-db/gett_algo/datasets/driver_offers/driver_offers_IL_2016_08/driver_id=',driver_id,'/ /mnt1/gett_algo/datasets/driver_offers/driver_offers_IL_2016_08/',driver_id) from (select count(*), cast(driver_id as varchar) driver_id from driver_offers_IL_2016_08 group by 2 having count(*) >= 1000)" > download_files.sh; sed -i 's/"//g' download_files.sh; chmod +x download_files.sh



