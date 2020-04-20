from pyspark import SparkContext
import os
from operator import itemgetter
import random,math,json
import itertools
import collections
import csv,datetime
import xgboost as xgb
import statistics, sys

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

def get_avg(itr):
    ratings = list(itr.values())
    avg = sum(ratings) / len(ratings)
    return avg

tic()
#folder_path = sys.argv[1]
input_file_path = '/Users/rubinakabir/Documents/553/yelp_train.csv'
#input_file_path = folder_path+'/yelp_train.csv'

sc = SparkContext('local[*]', 'task1')
rdd = sc.textFile(input_file_path)
# ['user_id', 'business_id', 'stars']
rdd = rdd.map(lambda line: line.split(",")).filter(lambda x: "user_id" not in x[0])
#  (user, business)
user_rows = rdd.map(lambda line: (line[0],( line[1],float(line[2]))))

test_input = '/Users/rubinakabir/Documents/553/yelp_val.csv'
#test_input = sys.argv[2]
test_data = sc.textFile(test_input,5).map(lambda line: line.split(",")).filter(lambda x: "user_id" not in x[0])\
     .map(lambda line: (line[0], (line[1],float(line[2]))))

folder_path = '/Users/rubinakabir/Downloads'
user_path = folder_path+'/user.json'

users = sc.textFile(user_path)
users = users.map(json.loads).map(lambda x: (x['user_id'],(x['review_count'],x['average_stars'],x['useful'],x['elite'],x['yelping_since'],x['fans'],x['funny'],x['cool']))).collectAsMap()

bus_path = folder_path+'/business.json'
business = sc.textFile(bus_path)
business = business.map(json.loads).map(lambda x: (x['business_id'],(x['stars'],x['review_count'],x['categories']))).collectAsMap()

def get_features(itr, switch):
    if switch == 0:
        get_business = business[itr[0]]
        stars = get_business[0]
        num_reviews = get_business[1]
        cats = get_business[2]
        if cats != None:
            if 'Food' in cats:
                food = 1
            else:
                food = 0
            if 'Restaurants' in cats:
                rest = 1
            else:
                rest = 0
        else:
            food, rest = 0, 0
        # feats = [num_reviews,stars]
        feats = [num_reviews, stars, food, rest]
    if switch == 1:
        user_id = itr[0]
        get_user = users[user_id]
        num_reviews = get_user[0]
        avg = get_user[1]
        useful = get_user[2]
        yelping_since = get_user[4]
        fans = get_user[5]
        yr, mt, d = int(yelping_since[0:4]), int(yelping_since[5:7]), int(yelping_since[8:])
        days = (datetime.date(2020, 3, 12) - datetime.date(yr, mt, d)).days
        if get_user[3] == 'None':
            elite = 0
        else:
            elite = len(get_user[3].split(','))

        # feats = [num_reviews, mean,median,mode,math.sqrt(std), pos, neg, neutral,
        #  elite,useful,days,fans]
        feats = [num_reviews, avg, useful, fans, days, elite]
    return (itr[0], feats)


uData = user_rows.groupByKey().map(lambda x: get_features(x, 1)).collectAsMap()
bData = user_rows.groupBy(lambda x: x[1][0]).map(lambda x: get_features(x, 0)).collectAsMap()


def get_training_data(itr, feats1, feats2):
    key = itr[0], itr[1][0]
    feats = feats1 + feats2
    return (key, feats)


xTrain = user_rows.map(lambda x: get_training_data(x, uData[x[0]], bData[x[1][0]])).collectAsMap()
yTrain = user_rows.map(lambda x: ((x[0], x[1][0]), x[1][1])).collectAsMap()
x_Train = [xTrain[x] for x in sorted(xTrain.keys())]
y_Train = [yTrain[x] for x in sorted(xTrain.keys())]

num_ufeats = len(list(uData.values())[1])
user_avg_feats = {}
for i in range(0, num_ufeats):
    user_avg_feats[i] = statistics.median([v[i] for k, v in uData.items()])
num_bfeats = len(list(bData.values())[1])
bus_avg_feats = {}
for i in range(0, num_bfeats):
    bus_avg_feats[i] = statistics.median([v[i] for k, v in bData.items()])


def testing_data(x):
    if uData.get(x[0]) == None:  # cold start user
        feat1 = [user_avg_feats[i] for i in range(0, num_ufeats)]

    else:
        feat1 = uData[x[0]]

    if bData.get(x[1][0]) == None:  # cold start business
        feat2 = [bus_avg_feats[i] for i in range(0, num_bfeats)]

    else:
        feat2 = bData[x[1][0]]

    out = feat1 + feat2
    key = x[0], x[1][0]
    return (key, out)


## num_reviews, mean, median,mode,math.sqrt(std), pos, neg, neutral,useful,elite

x_Test = test_data.map(lambda x: testing_data(x)).collectAsMap()
y_Test = test_data.map(lambda x: ((x[0], x[1][0]), x[1][1])).collectAsMap()

xTest = [x_Test[x] for x in sorted(x_Test.keys())]
yTest = [y_Test[x] for x in sorted(y_Test.keys())]

model = xgb.XGBRegressor(max_depth = 5, reg_alpha= 100)
model.fit(x_Train,y_Train)

yPred = model.predict(xTest)

model_based = dict([(k,yPred[ind])for ind,k in zip(range(0,len(yPred)),sorted(y_Test.keys()))])

#### ========== Item-Based ======= #####
actual_ratings = test_data.map(lambda x: ((x[0],x[1][0]), x[1][1])).collectAsMap() # {(user, bus): actual}
# ======= loading training data ======= #
data = user_rows.map(lambda x: (x[1][0], (x[0],x[1][1]))) # (bus, (user, rating))
training_data =  data.groupByKey().mapValues(dict).collectAsMap() #{bus: [ {user: rating} ] }
user_to_business = data.map(lambda x: (x[1][0],(x[0],x[1][1]))).groupByKey().mapValues(dict).collectAsMap() # {user: [ (bus, rating)]}
item_averages = {k: get_avg(v)for k,v in training_data.items()} # {item: avg}
true_train = user_rows.map(lambda x: ((x[0],x[1][0]), x[1][1])).collectAsMap()

correlations = {}


def get_prediction(itr):
    user_id = itr[0]
    i = itr[1][0]
    if user_to_business.get(user_id) == None:  # cold start user
        bus_avg = item_averages.get(i, 3.5)  # cold start business
        prediction = ((user_id, i), (bus_avg, 0))
        return prediction

    user_ratings = user_to_business[user_id]  # items rated by user
    if training_data.get(i) == None:  # cold start business
        bus_avg = item_averages.get(i, 4)
        prediction = ((user_id, i), (bus_avg, 0))
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
            if o < 0: o = 0.0  # 1.0
            if o > 5: o = 5.0
        prediction = ((user_id, i), (o, len(bus_weights)))
    else:
        bus_avg = item_averages.get(i, 3.5)
        prediction = ((user_id, i), (bus_avg, len(bus_weights)))

    return prediction

k = 30
item_based = test_data.map(lambda x: get_prediction(x)).collectAsMap() # ((user,bus),prediction)

mse = 0
o = []
for i in item_based.keys():
    mod = model_based[i]
    ib = item_based[i][0]

    if business[i[1]][2] != None and ('Restaurants' in business[i[1]][2] and 'Food' in business[i[1]][2]):
        score = (mod + ib) / 2

    else:
        score = mod
    out.append((i,score))
    m = (abs(score - actual_ratings[i]) ** 2)
    mse = mse + m
rmse = math.sqrt(mse / len(list(item_based.keys())))

#out_file = sys.argv[3]
out_file = '2.3_out.txt'
with open(out_file,'w') as out:
    wr = csv.writer(out, delimiter=',')
    wr.writerow(('user_id', 'business_id','prediction'))
    for row in o:
        out = (str(row[0][0]), str(row[0][1]) ,row[1])
        wr.writerow(out)
print('RMSE ',rmse)
print(toc())

