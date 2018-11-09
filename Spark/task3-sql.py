#!/usr/bin/env python
import sys
import csv
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string
spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()

open_violate=spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
open_violate.createOrReplaceTempView("open_violate")
result = spark.sql("Select license_type, sum(amount_due) as total, avg(amount_due) as average from open_violate group by license_type ")
result.select(format_string('%s\t%.2f, %.2f', result.license_type,result.total,result.average)).write.save("task3-sql.out",format="text")

