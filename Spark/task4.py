from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
	sc = SparkContext()
	lines = sc.textFile(sys.argv[1], 1)
	rows = lines.mapPartitions(lambda x: reader(x))
	rows = rows.map(lambda x: x[16])
	
	def state(x):
		if x == 'NY': 
			return ('NY', 1)
		else:
			return ('Other', 1)
	
	counts = rows.map(lambda x : state(x))
	
	result = counts.reduceByKey(add).map(lambda x: x[0] + '\t' + str(x[1]))
	
	result.saveAsTextFile("task4.out")
	
	sc.stop()



