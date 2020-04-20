from pyspark import SparkContext
import os
from operator import itemgetter
import random
import itertools
import collections
import csv
import json

os.environ['SPARK_HOME'] = '/Users/rubinakabir/Documents/553/spark-2.4.4-bin-hadoop2.7'
os.environ['PYTHONPATH'] = '/Users/rubinakabir/Documents/553/spark-2.4.4-bin-hadoop2.7/python'
os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.6'
sc = SparkContext('local[*]', 'task1')

def tic():
    # Homemade version of matlab tic and toc functions
    import time
    global startTime_for_tictoc
    startTime_for_tictoc = time.time()


def toc():
    import time
    if 'startTime_for_tictoc' in globals():
        return str(time.time() - startTime_for_tictoc) + " seconds."
    else:
        print("Toc: start time not set")

def charac_matrix(itr, users, business):
    bus_index = business[itr[1]]
    user_index = users[itr[0]]
    t = (bus_index,user_index)
    return t

def signature_matrix(itr,n,all_params):
    # input is grouped by bus/col (vals are row numbers
    out = []
    row = itr[0]
    for col in itr[1]:
        h_i = [hash_fun(x[0],x[1],row, n+1) for x in all_params]
        indexes = tuple(zip(range(0,len(h_i)), [col[0]]*len(h_i)))
        # index : (hash func, col), val
        potential = [((i,j), h_i[i]) for i,j in indexes]
        out.append(potential)
    return out


def hash_fun(a,p,row,n):
    return ((a * row) + p) % n


def Convert(tup, di):
    if type(tup) == set:
        for a, b,c in tup:
            di.setdefault(b, []).append((a,c))
        return di
    else:
        for a,b in tup:
            di.setdefault(b, []).append(a)
        return di

def hash_col(itr,n):
    row = []
    h_i = []
    for r in itr:
        row.append(r[0])
        h_i.append(r[1])
    # hash by (row values in that band): col associated
    index = sorted(tuple(zip(row,h_i)))
    hashed = tuple([x[1] for x in index])
    return hashed


def hash_bands(itr,n):
    dictionary = {}
    mini_matrix = Convert(itr[1], dictionary)
    # col : [ (r, h_i) ]
    hash_cols = [(col,hash_col(v,n)) for col,v in mini_matrix.items()]
    dictionary = {}
    hash_cols = Convert(hash_cols,dictionary)
    return hash_cols

def jaccard_similarity(x,y):
    inter = len(x & y)
    union = len(x) + len(y) - inter

    return inter / union
tic()
input_file_path = '/Users/rubinakabir/Documents/553/yelp_train.csv'
#input_file_path = source[1]
rdd = sc.textFile(input_file_path)
# ['user_id', 'business_id', 'stars']
rdd = rdd.map(lambda line: line.split(",")).filter(lambda x: "user_id" not in x[0])
#  (user, business)
user_rows = rdd.map(lambda line: (line[0], line[1]))

business = sorted(user_rows.values().distinct().collect())
business = dict(zip(business, range(0, len(business))))

users = sorted(user_rows.keys().distinct().collect())
n = len(users)
users = dict(zip(users, range(0, len(users))))

# NUMBER OF USERS = 11270
# NUMBER OF BUSINESS = 24732
characteristic_matrix = user_rows.map(lambda x: charac_matrix(x,users,business))

############# LSH  ################
num_hash = 39
b = 13
r = 3
all_params = [[550, 1935], [550, 4235], [550, 5236], [592, 1935], [592, 4235], [592, 5236], [619, 1935], [619, 4235],
 [619, 5236], [629, 1935], [629, 4235], [629, 5236], [649, 1935], [649, 4235], [649, 5236], [720, 1935],
 [720, 4235], [720, 5236], [750, 1935], [750, 4235], [750, 5236], [773, 1935], [773, 4235], [773, 5236],
 [814, 1935], [814, 4235], [814, 5236], [840, 1935], [840, 4235], [840, 5236], [860, 1935], [860, 4235],
 [860, 5236], [862, 1935], [862, 4235], [862, 5236], [924, 1935], [924, 4235], [924, 5236]]
row_lens = list(range(0, b*r, r))
band_dict = {}
c = 1
for i in range(0,len(row_lens)):
    if i == 0:
        ba = list(range(0,r))
    elif i+1 == len(row_lens):
        ba = list(range(row_lens[i],b*r))
    else:
        ba = list(range(row_lens[i],row_lens[i+1]))
    band_dict[c-1] = ba
    c = c + 1
band_dict = {v: k for k, vv in band_dict.items() for v in vv}

# row [(col,row)]
### using mapaprtitions prints in console
sig_matrix = characteristic_matrix.groupBy(lambda x: x[1]).map(lambda x:signature_matrix(x,n,all_params))\
    .flatMap(lambda x: x).flatMap(lambda x: x).reduceByKey(lambda a,b: a if a<b else b)

lsh = sig_matrix.map(lambda x: (band_dict[x[0][0]],(x[0][0],x[0][1],x[1]))).groupByKey().mapValues(set)\
    .map(lambda x: hash_bands(x,n)).collect()
candidates = []
for band in lsh:
    for cols in band.values():
        if len(cols) >=2:
            cols = sorted(cols)
            pairs = tuple(itertools.combinations(cols,2))
            candidates.append(pairs)


candidates = sc.parallelize(candidates).flatMap(lambda x:x).distinct()
actual_rdd = characteristic_matrix.groupByKey().mapValues(set).collectAsMap()
inverted_business = {k:v for v,k in business.items()}
similarities = candidates.map(lambda x: (x, jaccard_similarity(actual_rdd[x[0]], actual_rdd[x[1]])))
final_pairs = similarities.filter(lambda x: x[1] >= 0.5).keys().map(lambda x: tuple(sorted((inverted_business[x[0]],inverted_business[x[1]]))))
candidates = similarities.filter(lambda x: x[1] >= 0.5).map(lambda x: (tuple(sorted([inverted_business[x[0][0]],inverted_business[x[0][1]]])),x[1])).collect()

out_file = 't1_out.txt'
#out_file = source[1]
with open(out_file,'w') as out:
    wr = csv.writer(out, delimiter=',')
    wr.writerow(('business_id_1', 'business_id_2','similarity'))
    for line in candidates:
        #line = (str(line[0]), str(line[)
        wr.writerow(line)


# Jaccard Similarity
truth = '/Users/rubinakabir/Documents/553/pure_jaccard_similarity.csv'
true = sc.textFile(truth)
true = true.mapPartitions(lambda x : csv.reader(x)).filter(lambda x: "business_id_1" not in x[0]).map(lambda x: (x[0],x[1]))

tp = float(final_pairs.intersection(true).count())
fp = float(final_pairs.subtract(true).count()) # output is those Iman identified but arent candidates
fn = float(true.subtract(final_pairs).count()) # output those that are candidates that I DIDN't identify
print('FN',fn)
precision = tp/ (tp+fp)
recall = tp/(tp+fn)

print('Precision: ', precision)
print('Recall: ', recall )
time = toc()
print('Duration:', time)

