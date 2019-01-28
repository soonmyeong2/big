import sys
import re
from operator import add

from pyspark import SparkContext

def map_phase(x):
	x = re.sub('--', ' ', x)
	x = re.sub("'", '', x)
	return re.sub('[?!@#$\'",.;:()]', '', x).lower()

def countWord(line):
	global count_number
	if (line == "Tokyo"):
		count_number += 1
	return line.split(' ')

if __name__ == "__main__":
	if len(sys.argv) < 4:
		print >> sys.stderr, "Usage: wordcount <master> <inputfile> <outputfile>"
       		exit(-1)
	sc = SparkContext(sys.argv[1], "python_wordcount_sorted in bigdataprogrammiing")
	lines = sc.textFile(sys.argv[2],2)
	count_number = sc.accumulator(0)
	
	counts = lines\
	.flatMap(countWord)\
	.filter(lambda x: x!="Tokyo")\
	.map(lambda x: (x.lower(), 1))\
	.reduceByKey(lambda x,y:x+y)\
	.sortByKey(ascending=True)	
    
	counts.saveAsTextFile("hdfs://localhost:9000/output")
	print('Number of Tokyo : ', count_number.value)
  	sc.stop()
