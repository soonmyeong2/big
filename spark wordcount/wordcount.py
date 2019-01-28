#in[1]:


import sys
import re
from operator import add
from pyspark import SparkContext

# In[5]:


if __name__ == "__main__":
    if len(sys.argv) < 4:
            print(sys.stderr, "Usage: wordcount <master> <inputfile> <outputfile>")
            exit(-1)
            
    sc = SparkContext(sys.argv[1], "python_wordcount_sorted in bigdataprogramming")
    lines = sc.textFile(sys.argv[2],2) #partition number of 2

    counts = lines.filter(lambda x: x!="Tokyo").flatMap(lambda x: x.split(' ')).map(lambda x: (x.lower(), 1)).reduceByKey(lambda x,y : x+y).sortBy(lambda x: x[0])
    counts.saveAsTextFile("hdfs://localhost:9000/output");
    
    sc.stop()

