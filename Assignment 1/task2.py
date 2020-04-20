from pyspark import SparkContext
import os
import json
import sys
import random
import hashlib

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

    input_file_path = '/Users/rubinakabir/Downloads/yelp_dataset/review.json'
    #input_file_path = source[0]
    sc = SparkContext('local[*]', 'task2q')
    sc.setLogLevel("OFF")

    jsonrdd = sc.textFile(input_file_path)
    jsonrdd = jsonrdd.map(json.loads)
    jsonrdd = jsonrdd.map(lambda x: (x['business_id'],1))

    # Sparks default partitioning
    tic()
    reg_rdd = jsonrdd.reduceByKey(lambda a,b: a+b)
    count = reg_rdd.getNumPartitions()
    num_elements = reg_rdd.glom().map(len).collect()
    
    sp_top = reg_rdd.coalesce(1).sortBy(lambda k: (-k[1], k[0])).take(10)
    print(sp_top)
    sp_time = toc()
    print(sp_time)


    # Custom partitioning
    n_parts = 20
    jsonrdd = sc.textFile(input_file_path, n_parts)
    jsonrdd = jsonrdd.map(json.loads)
    jsonrdd = jsonrdd.map(lambda x: (x['business_id'],1))
    
    tic()
    new_rdd = jsonrdd.partitionBy(n_parts, lambda x: (hash(x) % 1234) %n_parts)
    count2 = new_rdd.getNumPartitions()
    num_elements2 = new_rdd.glom().map(len).collect()
    
    top_10 = new_rdd.reduceByKey(lambda a,b: a+b).coalesce(1).sortBy(lambda k: (-k[1], k[0])).take(10)
    print(top_10)
    cus_time = toc()
    print(cus_time)

    # Write to outputfile
    output = dict()
    output['default'] = {"n_partition": count, "n_items": num_elements,"exe_time":sp_time}
    output['customized'] = {"n_partition": count2, "n_items": num_elements2,"exe_time":cus_time}
    f_name = "task2_output.txt"
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
