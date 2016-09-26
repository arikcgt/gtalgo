from pyspark import SparkContext
from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("PythonSQL") \
        .getOrCreate()

    # print spark

    OFFERS_SQL = """
        select
        offers.sql.is_accepted,
        offers.sql.status,
        offers.sql.dim_driver_id,
        offers.sql.created_at,
        DAY(offers.sql.created_at) as day,
        offers.sql.dim_country_id,
        offers.sql.distance_from_order_on_creation,
        orders.ride_type,
        orders.class_type,
        orders.payment_type,
        orders.origin_city
        from fact_offers as offers.sql join fact_orders as orders
        on offers.sql.order_id = orders.order_id
        where offers.sql.dim_driver_id in (
            select offers.sql.dim_driver_id from (
                select dim_driver_id, count(*) from fact_offers
                where dim_time_id between now() - interval 2 month and now() - interval 1 month
                and dim_country_id = 'IL'
                and status not in ('Withdrawn', 'Not Shown')
                group by dim_driver_id
                order by 2 DESC
            ) as T
        )
        and offers.sql.dim_time_id between now() - interval 2 month and now() - interval 1 month
        and orders.dim_time_id between now() - interval 2 month and now() - interval 1 month
        and offers.sql.status not in ('Withdrawn', 'Not Shown')
        and offers.sql.dim_country_id = 'IL'
"""


    df_offers = spark.read.format("jdbc")\
        .option("url", "jdbc:mysql://dw-mysql-replica.gtforge.com/dw")\
        .option("user", "qa_user")\
        .option("password", "g28040sss")\
        .option("dbtable", "({}) as offers.sql".format(OFFERS_SQL))\
        .option("partitionColumn", "day").option("lowerBound", 0).option("upperBound", 31).option("numPartitions", 31)\
        .option("driver", "com.mysql.jdbc.Driver").load()

    df_offers.createOrReplaceTempView("offers.sql")
    df1 = spark.sql("select dim_driver_id, count(*) from offers.sql group by dim_driver_id")

    print df1
    df1.write.save("/tmp/offers3.csv", format="csv", header=True, mode="overwrite")

    res1 = spark.sql("select * from dwh.fact_offers_v where date_key between  '2016-08-01' and '2016-08-02' and country_symbol = 'IL'")
    res1.cache()
    resp = res1.write.partitionBy("driver_gk")
    resp.write.csv("s3://presto-db/gett_algo/datasets/driver_offers/test.csv", mode='overwrite', sep='\t', header=True)
    d2 = spark.read.csv("s3://presto-db/gett_algo/datasets/driver_offers/test.csv", inferSchema=True, header=True, sep='\t')


    #
    # lines = sc.textFile("offers.sql/sample.txt", 1)
    # counts = lines.flatMap(lambda x: x.split(' ')) \
    #     .map(lambda x: (x, 1)) \
    #     .reduceByKey(add)
    # output = counts.collect()
    # for (word, count) in output:
    #     print("%s: %i" % (word, count))

    spark.stop()
