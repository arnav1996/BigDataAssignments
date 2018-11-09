#!/usr/bin/env python
import sys
import csv
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string
from pyspark.sql.functions  import date_format

spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()
parking =spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
parking.createOrReplaceTempView("parking")
open_violate = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[2])
open_violate.createOrReplaceTempView("open_violate")
result=spark.sql("select  summons_number, plate_id, violation_precinct, violation_code, issue_date from parking where summons_number in ( SELECT summons_number from parking minus select summons_number from open_violate)")
result.select(format_string('%d\t%s, %d, %d,%s',result.summons_number,result.plate_id,result.violation_precinct,result.violation_code,date_format(result.issue_date,'yyyy-MM-dd'))).write.save("task1-sql.out",format="text")

