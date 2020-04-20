from pyspark import SparkContext
from pyspark.sql import SparkSession
import os,csv
from operator import itemgetter
import itertools,sys
import collections
from graphframes import *

os.environ["PYSPARK_SUBMIT_ARGS"] =  "--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11 pyspark-shell"
os.environ['SPARK_HOME'] = '/Users/rubinakabir/Documents/553/spark-2.4.4-bin-hadoop2.7'
os.environ['PYTHONPATH'] = '/Users/rubinakabir/Documents/553/spark-2.4.4-bin-hadoop2.7/python'
os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.6'
os.environ['JAVA_HOME'] = '/Library/Java/JavaVirtualMachines/jdk1.8.0_241.jdk/Contents/Home'
def tic():
    #Homemade version of matlab tic and toc functions
    import time
    global startTime_for_tictoc
    startTime_for_tictoc = time.time()

def toc():
    import time
    if 'startTime_for_tictoc' in globals():
        return "Elapsed time is " + str(time.time() - startTime_for_tictoc) + " seconds."
    else:
        print ("Toc: start time not set")
tic()
# ==== Load Data ==== #
input_file_path = 'power_input.txt'
#input_file_path = source[1]
sc = SparkContext('local[*]', 'task1')

edges = sc.textFile(input_file_path).map(lambda x: tuple(x.split(' '))).flatMap(lambda x: [ (x[0],x[1]), (x[1],x[0]) ])
vertices = edges.flatMap(lambda x: x).distinct().map(lambda x: (x,1))

spark = SparkSession(sc)
vertices = vertices.toDF(['id','1'])
edges = edges.toDF(['src','dst'])

graph = GraphFrame(vertices, edges)
communities = graph.labelPropagation(maxIter=5)
comm_rdd = communities.rdd.map(tuple).map(lambda x: (x[2],x[0])).groupByKey().mapValues(sorted).collect()

comm_rdd.sort(key = lambda x : (len(x[1]),x[1]))

out_file = 'task1_out.txt'
#out_file = source[2]
with open(out_file,'w') as out:
    for line in comm_rdd:
        out.write(str(line[1])[1:-1])
        out.write('\n')

print(toc())
print('Number of communities: ', len(comm_rdd))
