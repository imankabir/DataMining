from pyspark import SparkContext
import os
from operator import itemgetter
import random,math,statistics
import itertools
import collections
import csv

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

def get_avg(itr):
    ratings = list(itr.values())
    avg = sum(ratings) / len(ratings)
    return avg

def get_prediction(itr):
    user_id = itr[0]
    i = itr[1][0]
    if user_to_business.get(user_id) == None:  # cold start user
        bus_avg = item_averages.get(i, 3.5)  # cold start business
        prediction = ((user_id, i), bus_avg)
        return prediction

    user_ratings = user_to_business[user_id]  # items rated by user
    if training_data.get(i) == None:  # cold start business
        bus_avg = item_averages.get(i, 4)
        prediction = ((user_id, i), bus_avg)
        return prediction
    # =========== Non-cold start ============== #
    # Filter out business not rated by user from training data
    i_ratings = training_data[i]  # {user:rating, ..}
    avg_i = item_averages[i]
    bus_i = i_ratings.keys()  # set of users who rated item i
    bus_weights = []

    for j in user_ratings.keys():
        i_d = (i, j)
        if j == i: continue
        if i_d in correlations:  # if correlation already calculated
            bus_weights.append((correlations[i_d], j))
            continue
        if (j, i) in correlations:  # if correlation already calculated
            bus_weights.append((correlations[(j, i)], j))
            continue
        j_ratings = training_data[j]  # get bus j user ratings
        bus_j = set([x for x in j_ratings.keys()])
        # get users who rated j
        intersection = (bus_i & bus_j)  # set of users who rated i and j

        # ======== Correlation Calculations ======== #
        if len(intersection) > 28:
            avg_j = item_averages[j]
            numerator = sum([((i_ratings[x] - avg_i) * (j_ratings[x] - avg_j)) for x in intersection])

            ii = math.sqrt(sum([((i_ratings[x] - avg_i) ** 2) for x in intersection]))
            jj = math.sqrt(sum([((j_ratings[x] - avg_j) ** 2) for x in intersection]))
            if float(ii) == 0.0 or float(jj) == 0.0:
                weight_ij = 0
                correlations[i_d] = weight_ij
                continue
            else:
                weight_ij = numerator / (ii * jj)
                if weight_ij > 0:
                    correlations[i_d] = weight_ij
                    bus_weights.append((weight_ij, j))

    # ======== Prediction Calculation ======== #
    if len(bus_weights) > 10:
        k_neighbors = sorted(bus_weights, reverse=True)[0:k]  # getting closest business neighbors
        k_neighbors = {x[1]: x[0] for x in k_neighbors}
        neighbors = k_neighbors.keys()
        numerator = sum([(user_ratings[n] * k_neighbors[n]) for n in neighbors])
        denom = sum([abs(k_neighbors[n]) for n in neighbors])
        if denom == 0.0:
            o = 0
        else:
            o = numerator / denom
            if o < 0: o = 0.0 #1.0
            if o > 5: o = 5.0
        prediction = ((user_id, i), o)
    else:
        bus_avg = item_averages.get(i, 3.5)
        prediction = ((user_id, i), bus_avg)

    return prediction

tic()
input_file_path = '/Users/rubinakabir/Documents/553/yelp_train.csv'
#input_file_path = source[0]
rdd = sc.textFile(input_file_path).map(lambda line: line.split(",")).filter(lambda x: "user_id" not in x[0])
user_rows = rdd.map(lambda line: (line[0], (line[1],float(line[2]))))

# ======= loading test data ======= #
test_input = '/Users/rubinakabir/Documents/553/yelp_val.csv'
#test_input = source[1]
test_data = sc.textFile(test_input).map(lambda line: line.split(",")).filter(lambda x: "user_id" not in x[0])\
        .map(lambda line: (line[0], (line[1],float(line[2])))) # (user, (bus,rating))
actual_ratings = test_data.map(lambda x: ((x[0],x[1][0]), x[1][1])).collectAsMap() # {(user, bus): actual}

# ======= loading training data ======= #
data = user_rows.map(lambda x: (x[1][0], (x[0],x[1][1]))) # (bus, (user, rating))
training_data =  data.groupByKey().mapValues(dict).collectAsMap() #{bus: [ {user: rating} ] }
user_to_business = data.map(lambda x: (x[1][0],(x[0],x[1][1]))).groupByKey().mapValues(dict).collectAsMap() # {user: [ (bus, rating)]}
item_averages = {k: get_avg(v)for k,v in training_data.items()} # {item: avg}

# ====== PC Item-Based  CF ======= #
correlations = {} # used to store correlations are we go through data to avoid redundant calculations
k = 30
predictions = test_data.map(lambda x: get_prediction(x)) # ((user,bus),prediction)
print('calculating...')
preds = predictions.collect()
mse = predictions.map(lambda x: (abs(x[1] - actual_ratings[x[0]]))**2).collect()

out_file = '2.1_out.txt'
#out_file = source[2]
with open(out_file,'w') as out:
    wr = csv.writer(out, delimiter=',')
    wr.writerow(('user_id', 'business_id','prediction'))
    for line in preds:
        line = (str(line[0][0]), str(line[0][1]),line[1])
        wr.writerow(line)
mse = sum(mse) / len(mse)
rmse = math.sqrt(mse)
print('RMSE: ',rmse)
print(toc())
