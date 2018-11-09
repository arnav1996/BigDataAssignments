#!/usr/bin/env python
import sys
import csv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import format_string
spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()

parking =spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
parking.createOrReplaceTempView("parking")
temp = parking.select(F.when(parking.registration_state=='NY','NY').otherwise('Other'))
df=temp
df = df.withColumnRenamed("CASE WHEN (registration_state = NY) THEN NY ELSE Other END", "state")
df.createOrReplaceTempView("df")
result = spark.sql("select state, count(*) as total from df group by state order by state")
result.select(format_string('%s\t%d', result.state,result.total)).write.save("task4-sql.out",format="text")

