from pyspark import SparkContext
import os
from operator import itemgetter
import random,math,json
import itertools
import collections
import csv,datetime
import xgboost as xgb
import statistics

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

os.environ['SPARK_HOME'] = '/Users/rubinakabir/Documents/553/spark-2.4.4-bin-hadoop2.7'
os.environ['PYTHONPATH'] = '/Users/rubinakabir/Documents/553/spark-2.4.4-bin-hadoop2.7/python'

os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.6'
os.environ['JAVA_HOME'] = '/Library/Java/JavaVirtualMachines/jdk1.8.0_241.jdk/Contents/Home'
tic()
sc = SparkContext('local[*]', 'task1')
#folder_path = sys.argv[1]
input_file_path = '/Users/rubinakabir/Documents/553/yelp_train.csv'
#input_file_path = folder_path+'/yelp_train.csv'
rdd = sc.textFile(input_file_path)
# ['user_id', 'business_id', 'stars']
rdd = rdd.map(lambda line: line.split(",")).filter(lambda x: "user_id" not in x[0])
#  (user, business)
user_rows = rdd.map(lambda line: (line[0],( line[1],float(line[2]))))

test_input = '/Users/rubinakabir/Documents/553/yelp_val.csv'
#test_input = sys.argv[2]
test_data = sc.textFile(test_input).map(lambda line: line.split(",")).filter(lambda x: "user_id" not in x[0])\
     .map(lambda line: (line[0], (line[1],float(line[2]))))
folder_path = '/Users/rubinakabir/Downloads'
# ======= Extract features from other data sources ====== #
user_path = folder_path+'/user.json'

users = sc.textFile(user_path)
users = users.map(json.loads).map(lambda x: (x['user_id'],(x['review_count'],x['average_stars'],x['useful'],x['elite'],x['yelping_since'],x['fans']))).collectAsMap()

bus_path = folder_path+'/business.json'
business = sc.textFile(bus_path)
business = business.map(json.loads).map(lambda x: (x['business_id'],(x['stars'],x['review_count']))).collectAsMap()


def get_features(itr, switch):
    if switch == 0:
        get_business = business[itr[0]]
        stars = get_business[0]
        num_reviews = get_business[1]
        feats = [num_reviews, stars]
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

        feats = [num_reviews, avg, useful, fans, days, elite]
    return (itr[0], feats)


uData = user_rows.groupByKey().map(lambda x: get_features(x, 1)).collectAsMap()
bData = user_rows.groupBy(lambda x: x[1][0]).map(lambda x: get_features(x, 0)).collectAsMap()


def training_data(itr, feats1, feats2):
    key = itr[0], itr[1][0]
    feats = feats1 + feats2
    return (key, feats)


xTrain = user_rows.map(lambda x: training_data(x, uData[x[0]], bData[x[1][0]])).sortByKey().collect()
yTrain = user_rows.map(lambda x: ((x[0], x[1][0]), x[1][1])).sortByKey().collect()
xTrain = [x[1] for x in xTrain]
yTrain = [x[1] for x in yTrain]

# ======= Get Averages for cold start users and cold start business feats======= #
num_ufeats = len(list(uData.values())[1])
user_avg_feats = {}
for i in range(0, num_ufeats):
    user_avg_feats[i] = statistics.median([v[i] for k, v in uData.items()])
num_bfeats = len(list(bData.values())[1])
bus_avg_feats = {}
for i in range(0, num_bfeats):
    bus_avg_feats[i] = statistics.median([v[i] for k, v in bData.items()])


# ======= Get Testing Data Features ====== #
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
    return ((x[0], x[1][0]), out)


## num_reviews, mean, median,mode,math.sqrt(std), pos, neg, neutral,useful,elite
x_Test = test_data.map(lambda x: testing_data(x)).collectAsMap()
y_Test = test_data.map(lambda x: ((x[0], x[1][0]), x[1][1])).collectAsMap()

xTest = [x_Test[x] for x in sorted(x_Test.keys())]
yTest = [y_Test[x] for x in sorted(y_Test.keys())]

# ======= Model- Based CF using XGB Regressor ======= #
model = xgb.XGBRegressor(max_depth=1, reg_alpha=1)
xgb.XGBRegressor()
model.fit(xTrain, yTrain)

yPred = model.predict(xTest)

out_file = '2.1_out.txt'
#out_file = sys.argv[3]
with open(out_file,'w') as out:
    wr = csv.writer(out, delimiter=',')
    wr.writerow(('user_id', 'business_id','prediction'))
    for ind,row in zip(range(0,len(yPred)),sorted(x_Test.keys())):
        out = (str(row[0]), str(row[1]) ,yPred[ind])
        wr.writerow(out)

s = 0
for i in range(0, len(yTest)):
    if yPred[i] < 0.0: yPred[i] = 0.0
    if yPred[i] > 5.0: yPred[i] = 5.0
    s = s + (abs(yPred[i] - yTest[i]) ** 2)
rmse = math.sqrt(s / len(yTest))
print('Test RMSE ', rmse)
print('====================================')
print('Duration:',toc())