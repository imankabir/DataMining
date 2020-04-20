from pyspark import SparkContext
import os
import json
import sys

def tic():
    #Homemade version of matlab tic and toc functions
    import time
    global startTime_for_tictoc
    startTime_for_tictoc = time.time()

def toc():
    import time
    if 'startTime_for_tictoc' in globals():
        return str(time.time() - startTime_for_tictoc) + " seconds."
    else:
        print ("Toc: start time not set")


def main(*source):
    os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
    os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.6'

    #input_file_path = source[1]
    sc = SparkContext('local[*]', 'task3')
    n_parts = 50
    sc.setLogLevel("OFF")
    input_file_path = '/Users/rubinakabir/Downloads/yelp_dataset/business.json'
    busrdd = sc.textFile(input_file_path,n_parts)
    busrdd = busrdd.map(json.loads)

    #input_file_path = source[0]
    input_file_path = '/Users/rubinakabir/Downloads/yelp_dataset/review.json'
    jsonrdd = sc.textFile(input_file_path,n_parts)
    jsonrdd = jsonrdd.map(json.loads)

    # A. Average stars for each city + B. top 10 - SPARK VERSION
    tic()
    n_parts = 50
    city = busrdd.map(lambda x: (x['business_id'], x['city'])).partitionBy(n_parts,
          lambda x: (hash(x) % 1234) %n_parts)
    stars = jsonrdd.map(lambda x: (x['business_id'], x['stars'])).partitionBy(n_parts,
            lambda x: (hash(x) % 1234) %n_parts)

    initial = (0, 0)
    averages = stars.join(city).map(lambda x: (x[1][1], x[1][0]))
    averages= averages.aggregateByKey(initial, lambda a,b: (a[0] + b,a[1] + 1),
          lambda a,b: (a[0] + b[0], a[1] + b[1]),numPartitions=10).mapValues(lambda v: v[0]/v[1]).coalesce(1)#.cache()
    averages.first()
    # B. Top 10 - PYTHON VERSION
    tic()
    python_collect = averages.collect()
    python_collect.sort(key=lambda k: (-k[1], k[0]))
    top_10_python = python_collect[0:10]
    print(top_10_python)
    m2 = toc()

    tic()
    top_10_spark = averages.sortBy(lambda k: (-k[1], k[0])).take(10)
    print(top_10_spark)
    m1 = toc()
    print(m1)


    #f_name=source[2]
    f_name = "task3a_output.txt"
    with open(f_name, "w") as t2:
        t2.write('city,stars\n')
        print('here')
        for bus in python_collect:
            strs = ",".join(str(x) for x in bus)
            t2.write(strs + '\n')

    #f_name=source[3]
    f_name = "task3b_output.txt"
    output = dict()
    output['m1'] = m1
    output["m2"] =  m2
    print('about to write')
    f_output = open(f_name, "w")
    f_output.write(json.dumps(output))
    print('output done')

if __name__ == '__main__':
    try:
        main()
        #main(sys.argv[1],sys.argv[2],sys.argv[3)
    except:
        print("error")
