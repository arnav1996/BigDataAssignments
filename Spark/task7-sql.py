#!/usr/bin/env python
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string

spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option",
                                                                              "some-value").getOrCreate()

parking = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[1])
parking.createOrReplaceTempView("parking")

result = spark.sql("select w1.violation_code,(w1.weekendCount / 8) as weekendAvg,(w2.weekdayCount / 23) as weekdayAvg from (select violation_code, sum(indicator) as weekendCount from (select violation_code, case when DAY(issue_date) IN (5,6,12,13,19,20,26,27) then 1 else 0 end as indicator from parking) group by violation_code) w1, (select violation_code, sum(indicator) as weekdayCount from (select violation_code, case when DAY(issue_date) NOT IN (5,6,12,13,19,20,26,27) then 1 else 0 end as indicator from parking) group by violation_code) w2 where w1.violation_code = w2.violation_code")

result.select(format_string('%s\t%.2f, %.2f', result.violation_code,result.weekendAvg,result.weekdayAvg)).write.save("task7-sql.out",format="text")






