#!/usr/bin/env python
import sys
import csv
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string
spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()

parking =spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
parking.createOrReplaceTempView("parking")
result = spark.sql("select violation_code, count(violation_code) as total from parking group by violation_code")
result.select(format_string('%d\t%d', result.violation_code,result.total)).write.save("task2-sql.out",format="text")

