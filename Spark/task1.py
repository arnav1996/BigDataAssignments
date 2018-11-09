from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
	sc = SparkContext()
	lines = sc.textFile(sys.argv[1], 1)
	rows = sc.textFile(sys.argv[2],1)
	
	lines = lines.mapPartitions(lambda x: reader(x))
	rows = rows.mapPartitions(lambda x: reader(x))
	
	parking = lines.filter(lambda line: len(line)>1) \
			.map(lambda line: (line[0], str(line[14]) + ', ' + str(line[6]) + ', ' + str(line[2]) + ', ' + str(line[1]))) 
			
	
	open = 	rows.filter(lambda line: len(line)>1) \
			.map(lambda line: (line[0], str(line[1]) + ', ' + str(line[5]) + ', ' + str(line[7]) + ', ' + str(line[9])))
	
	both=parking.join(open)

	result=parking.subtractByKey(both)
	
	output = result.map(lambda r:"\t".join([str(c) for c in r]))
	
	output.saveAsTextFile("task1.out")
	
	sc.stop()

