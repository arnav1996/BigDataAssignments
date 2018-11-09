#!/usr/bin/env python
import sys
import csv
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string
spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()

parking =spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
parking.createOrReplaceTempView("parking")
result = spark.sql("select * from (select registration_state,plate_id, count(violation_code) as total  from parking group by plate_id, registration_state) g order by g.total desc, g.plate_id limit 20")
result.select(format_string('%s, %s\t%d', result.plate_id,result.registration_state,result.total)).write.save("task6-sql.out",format="text")

