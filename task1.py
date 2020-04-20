from pyspark import SparkContext
import pyspark
import os
import json
import sys
import random
from operator import itemgetter

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


def main(*source):
    print(source)
    #os.environ['SPARK_HOME'] = '/Users/rubinakabir/Documents/553/spark-2.4.4-bin-hadoop2.7'
    #os.environ['PYTHONPATH'] = '/Users/rubinakabir/Documents/553/spark-2.4.4-bin-hadoop2.7/python'

    os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
    os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.6'

    #input_file_path = source[0]
    input_file_path = '/Users/rubinakabir/Downloads/yelp_dataset/review.json'
    sc = SparkContext('local[*]', 'task1')

    sc.setLogLevel("OFF")
    n_parts = 30
    jsonrdd = sc.textFile(input_file_path, n_parts)
    jsonrdd = jsonrdd.map(json.loads).map(lambda x: (x['user_id'], (int(x['date'][0:4]), x['business_id']))
            ).partitionBy(n_parts,lambda x: (hash(x) % 1234) %n_parts )
    
    # Set up Data Frame for A C D
    acd = jsonrdd.mapValues(lambda x: (1)).reduceByKey(lambda a,b:a+b).sortBy(lambda k: (-k[1], k[0]))
    
    # D. Top 10 users
    d = acd.take(10)
    
    # A. number of reviews
    accum = sc.accumulator(0)
    acd.foreach(lambda x: accum.add(x[1]))
    a = accum.value

    # C. Number of distinct users who wrote review
    c = acd.count()

    # B. The number of reviews in 2018
    b = jsonrdd.filter(lambda x: 2018 == x[1][0]).count()

    # Set up data frame for e and f
    e_f = jsonrdd.map(lambda x: (x[1][1], 1)).reduceByKey(lambda a, b: a + b)\
          .coalesce(2).sortBy(lambda k: (-k[1], k[0]))
    
    # F. Top 10 business with most reviews
    f = e_f.take(10)
    
    # E. Number of distinct businesses
    e = e_f.count()


    # Write to outputfile
    output = dict()
    output['n_review'] = a
    output["n_review_2018"] = b
    output['n_user'] = c
    output['top10_user'] = [[x[0], x[1]] for x in d]
    output['n_business'] = e
    output['top10_business'] = [[x[0], x[1]] for x in f]
    f_name = "task1_output.txt"
    #f_name = source[1]
    print('about to write')
    f_output = open(f_name, "w")
    f_output.write(json.dumps(output))
    print('output done')

if __name__ == '__main__':
    try:
        main()
        #main(sys.argv[1],sys.argv[2])
    except:
        print("error")
