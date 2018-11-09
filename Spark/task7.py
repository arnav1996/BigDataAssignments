import sys
from csv import reader
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("task7") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
sc = spark.sparkContext

allviolation = sc.textFile(sys.argv[1], 1)
allviolation = allviolation.mapPartitions(lambda x: reader(x))
weekendList = [5,6,12,13,19,20,26,27]
violationCodeWithWeekendCount = allviolation.map(lambda x: (x[2], 1 if int(x[1].split("-")[2]) in weekendList else 0)).reduceByKey(lambda x,y: float(x)+float(y))#.filter(lambda x: int(x[1]) != 0)
violationCodeWithWeekdayCount = allviolation.map(lambda x: (x[2], 1 if int(x[1].split("-")[2]) not in weekendList else 0)).reduceByKey(lambda x,y: float(x)+float(y))#.filter(lambda x: int(x[1]) != 0)
violationCodeWithWeekendCountAvg = violationCodeWithWeekendCount.map(lambda x: (x[0],float(x[1])/8.0))
violationCodeWithWeekdayCountAvg = violationCodeWithWeekdayCount.map(lambda x: (x[0],float(x[1])/23.0))
violationCodeAvg = violationCodeWithWeekendCountAvg.join(violationCodeWithWeekdayCountAvg)
violationCodeAvg.map(lambda x: "{0}\t{1:.2f}, {2:.2f}".format(x[0],float(x[1][0]),float(x[1][1]))).saveAsTextFile("task7.out")


