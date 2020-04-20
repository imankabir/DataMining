from pyspark import SparkContext
import os, itertools
import json
import sys
import random
import csv
import collections

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

def get_p(index, itr,total_distinct_reviews,support):
    count = 0
    for user in itr:
        l = len(user[1])
        count = count + l
    yield (index, (count*support)/total_distinct_reviews)


def pcy_pass1(idx, itr, support):
    singleton_counts = collections.Counter()
    bucket_counts = {}
    bm1_pairs = {}
    for user in itr:
        reviews = user[1]
        reviews = sorted(reviews)
        # update dict for single itemcounts
        singleton_counts = collections.Counter(reviews) + singleton_counts

        # Generate pairs present in list and create hash table
        pair_list = list(itertools.combinations(reviews, 2))
        # hash pairs into buckets then update counter for bitmap
        pair_list = [(int(i[0]),int(i[1])) for i in pair_list]
        hash_buckets = dict(collections.Counter(list(map(lambda x: (sum(x))%3,pair_list))))
        for k,v in hash_buckets.items():
            # Check if frequent bucket
            if bucket_counts.get(k,0) >= support:
                bm1_pairs[k] = 1
                continue
            bucket_counts[k] = bucket_counts.get(k,0) + v
            if bucket_counts.get(k,0) >= support:
                bm1_pairs[k] = 1

    # Find local frequent singletons
    frequent_singletons = list(dict(filter(lambda elem: elem[1]>=support , singleton_counts.items())).keys())
    # Generate bitmap from pair counts
    output = {'frequent':frequent_singletons, 'bitmap':bm1_pairs}
    yield (idx,output)


def f(x): return x


def pcy_passk(idx,itr, size, support,pcy_passkminus1):
    if type(pcy_passkminus1) != dict:
        output = 'DONE FOR THIS PARTITION'
        yield (idx, output)
        return
    frequent_kminus1 = pcy_passkminus1['frequent']
    bit_mapk = pcy_passkminus1['bitmap']
    candidate_k = {}
    bit_mapKplus1 = {}
    bucket_counts = {}
    if len(list(bit_mapk.keys())) > 0:
        for user in itr:
            reviews = user[1]
            reviews = sorted(reviews)
            if size == 2:
                reviews = list(filter(lambda e: e in frequent_kminus1, reviews))
                user_szk = list(itertools.combinations(reviews, 2))
                for itemset in user_szk:
                    int_set = tuple((int(i) for i in itemset))
                    bucket = sum(int_set) % 3
                    if bit_mapk.get(bucket, 0) == 1:
                        candidate_k[itemset] = candidate_k.get(itemset, 0) + 1
            if size > 2:
                # apply monotonocity to remove ids from list
                ## if itemset  i  of size k-1 is not frequent then no itemset containing
                ## i of size K can be frequent ###
                user_szkminus1 = list(itertools.combinations(reviews,size-1))
                user_szkminus1 = list(filter(lambda e: e in frequent_kminus1, user_szkminus1))
                reviews = list(dict.fromkeys(list(sum(user_szkminus1, ()))))
                reviews = sorted(reviews)
                user_szk = list(itertools.combinations(reviews, size))
                for itemset in user_szk:
                    check = list(itertools.combinations(itemset,size-1))
                    l_check = len(check)
                    check_km1 = len(list(filter(lambda e: e in frequent_kminus1, check)))
                    if l_check != check_km1:
                        continue
                    else:
                        int_set = tuple((int(i) for i in itemset))
                        bucket = sum(int_set) % 3
                        if bit_mapk.get(bucket,0) == 1:
                            candidate_k[itemset] = candidate_k.get(itemset,0) + 1

            if len(reviews) > size:
                user_kplus1 = list(itertools.combinations(reviews,size+1))
                user_kplus1 = list(map(lambda x: (int(i) for i in x) ,user_kplus1))
                hash_buckets = dict(collections.Counter(list(map(lambda x: sum(x) % 3, user_kplus1))))
                for k,v in hash_buckets.items():
                    if bucket_counts.get(k,0) > support:
                        bit_mapKplus1[k] = 1
                        continue
                    bucket_counts[k] = bucket_counts.get(k,0) + v
                    if bucket_counts.get(k, 0) > support:
                        bit_mapKplus1[k] = 1

        candidate_k = list(dict(filter(lambda elem: elem[1] >= support, candidate_k.items())).keys())
        output = {'frequent':candidate_k,'bitmap':bit_mapKplus1}
        yield (idx, output)
    else:
        output = 'DONE FOR THIS PARTITION'
        yield (idx, output)


def son_map_phase2(idx,itr,local_frequents):
    max_size = len(list(local_frequents.keys()))
    evaluate = collections.Counter()
    for user in itr:
        reviews = user[1]
        reviews.sort()
        reviews = collections.Counter(list(filter(lambda e: e in local_frequents[1], reviews)))

        if len(reviews) > 0:
            evaluate = evaluate + reviews
        for size in range(2,max_size+1):
            frequent_sizek = local_frequents[size]
            user_sizek = list(itertools.combinations(reviews,size))

            # Filter ones not in list
            user_sizek = collections.Counter(list(filter(lambda e: e in frequent_sizek,user_sizek)))
            if len(user_sizek) > 0:
                evaluate = evaluate + user_sizek
    yield (dict(evaluate))


os.environ['SPARK_HOME'] = '/Users/rubinakabir/Documents/553/spark-2.4.4-bin-hadoop2.7'
os.environ['PYTHONPATH'] = '/Users/rubinakabir/Documents/553/spark-2.4.4-bin-hadoop2.7/python'

os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.6'

sc = SparkContext('local[*]', 'task1')
sc.setLogLevel("OFF")
n_parts = 2
support = 4
#support = int(sys.argv[2])

# Load RDD
input_file_path = '/Users/rubinakabir/Documents/553/small2.csv'
#input_file_path = sys.argv[3]
# output_file_path = sys.argv[4]
output_file_path = 'out.txt'

case = 1
#case = int(sys.argv[1])
tic()
if  case == 1:
##########################
####### CASE 1 ###########
##########################
    rdd = sc.textFile(input_file_path, n_parts)
    rdd = rdd.map(lambda line: line.split(",")).filter(lambda x: "user_id" not in x[0])\
       .map(lambda line: (line[0],line[1])).partitionBy(n_parts,lambda x: int(x) % n_parts)

    # Remove duplicate user_id,business_id combo
    rdd = rdd.distinct()
    total_distinct_reviews = rdd.count()
    baskets = rdd.map(lambda x : (x[0], [x[1]])).reduceByKey(lambda p,q: p+q)

if case == 2:
##########################
####### CASE 2 ###########
##########################
    tic()
    rdd = sc.textFile(input_file_path, n_parts)
    rdd = rdd.map(lambda line: line.split(",")).filter(lambda x: "user_id" not in x[0])\
       .map(lambda line: (line[1],line[0])).partitionBy(n_parts,lambda x: int(x)**2 % n_parts)

    # Remove duplicate business_id user_id combo
    rdd = rdd.distinct()
    total_distinct_reviews = rdd.count()
    baskets = rdd.map(lambda x : (x[0], [x[1]])).reduceByKey(lambda p,q: p+q)


# get scaled threshold for each partition
p_partition = baskets.mapPartitionsWithIndex(lambda idx, itr: get_p(idx, itr, total_distinct_reviews, support),
              preservesPartitioning=True).collect()

###### MAPREDUCE PHASE 1: FINAL LOCAL CANDIDATES ########
tic()
k = 1
final_candidates = {}
while True:
    if k == 1:
        passk = baskets.mapPartitionsWithIndex(lambda idx, itr: pcy_pass1(idx, itr, p_partition[idx][1])
                , preservesPartitioning=True).collect()
        #print('PCY Pass 1: Local Frequent Singleton + Pairs BitMap', passk)
        singletons = []
        for part in passk:
            singletons.append(part[1]['frequent'])
        singletons = list(dict.fromkeys(list(sum(singletons, []))))
        final_candidates[1] = sorted(singletons)
        k = k + 1
        continue
    else:
        passk = baskets.mapPartitionsWithIndex(lambda idx, itr: pcy_passk(idx, itr, k ,p_partition[idx][1],
            passk[idx][1]), preservesPartitioning=True).collect()
        #print('PCY Pass '+ str(k)+' Local Frequent of Size '+str(k)+' + Size ',str(k+1),' Bitmap',passk)

        # Check if continue to find k+1 itemsets
        check = 0
        szk = []
        for part in passk:
            if 'DONE' in part[1] or len(part[1]['frequent']) == 0 or len(list(part[1]['bitmap'].keys())) == 0:
                check = check + 1
                continue
            else:
                szk.append(part[1]['frequent'])
        if check == n_parts:
            break
        szk = list(dict.fromkeys(list(sum(szk, []))))
        final_candidates[k] = sorted(szk)
        k = k + 1
# Write
print('Final Local Candidates',)
for k,v in final_candidates.items():
    print('Size', k)
    print('Len',len(v))
    print(v)
    print('---------')

with open(output_file_path,"w") as out:
    out.write('Candidates: \n')
    for k,v in final_candidates.items():
        c = 0
        for vv in v:
            vv = str(vv)
            if c == 0:
                if k == 1:
                    vv = '(\'%s\')' % vv
                    out.write(vv)
                    c = 3
                    continue
                out.write(vv)
                c = 3
                continue
            if k == 1:
                vv =',(\'%s\')' % vv
            else:
                vv = ',' + vv
            out.write(vv)
        #out.write('\n'+str(len(v)))
        out.write('\n\n')

########### MapReduce Phase 2: Find True Frequent Itemsets ###########
son_map_ph2 = baskets.mapPartitionsWithIndex(lambda idx,itr: son_map_phase2(idx,itr, final_candidates),preservesPartitioning=True) \
    .flatMap(lambda x: [(k,v) for k, v in x.items()]).reduceByKey(lambda a,b: a+b)\
    .filter(lambda x: x[1]>= support).keys().collect()

frequents = collections.defaultdict(list)
for elem in son_map_ph2:
    if type(elem)!= tuple :
        frequents[1].append(elem)
    else:
        get_len = len(elem)
        frequents[get_len].append(elem)
final_frequents = {k: sorted(v) for k,v in frequents.items()}
print(final_frequents)

# Write to output file
with open(output_file_path,"a+") as out:
    out.write('Frequent Itemsets: \n')
    print('Final Itemsets')

    for k,v in final_frequents.items():
        c = 0
        print('Size', k)
        print(len(v))
        for vv in v:
            vv = str(vv)
            if c == 0:
                if k == 1:
                    vv = '(\'%s\')' % vv
                    out.write(vv)
                    c = 3
                    continue
                out.write(vv)
                c = 3
                continue
            if k == 1:
                vv =',(\'%s\')' % vv
            else:
                vv = ',' + vv
            out.write(vv)
        #out.write('\n'+str(len(v)))
        out.write('\n\n')
    #out.write('\n\n')

print("Duration: ", toc())
