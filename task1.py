from blackbox import BlackBox
import binascii
import random,csv
# === Global Variables === #
global filter
filter = [0] * 69997
global filter_len
filter_len= 69997
stream_size = 100
num_of_asks = 30

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

#def hash_funcs():    a = rand.

input_file = 'users.txt'
bx = BlackBox()
true_seen = set()
def goodness_of_fit_sum(j,buckets):
    b_j = buckets[j]*((buckets[j] + 1)/2)
    return b_j

def hash_1(user):
    h_1 = ((user * 3 + 3) % (filter_len * 1515)) % filter_len
    return h_1

def hash_2(user):
    h_2 = ((user * 7 + 87) % (filter_len * 177)) % filter_len
    return h_2

def myhash(s):
    result = []
    hash_function_list = [hash_1, hash_2]
    for f in hash_function_list:
        result.append(f(s))
    return result

buckets_1 = {}
buckets_2 = {}
filter_dict = {}
out_fpr = []
init = 0
for i in range(0,num_of_asks):
    stream_users = bx.ask(input_file,stream_size)
    # === Convert user_id to int === #
    stream_users= [int(binascii.hexlify(u.encode('utf8')),16) for u in stream_users]
    # === Test if in Set === #
    fp = 0
    tn = 0
    for user,k in zip(stream_users,range(0,100)):
        h_1 = ((user*3 + 3) % (filter_len * 1515)) % filter_len
        h_2 =  ((user*7 + 87) % (filter_len*177)) % filter_len
        if i == 0 and init == 0: # first element seen -- don't need to test
            filter[h_1] = 1
            filter[h_2] = 1
            true_seen.add(user)
            init = 1
            continue
        if filter[h_1] == 1 and filter[h_2] == 1: # Classifed as in object s
            if user in true_seen: # check if user actually in s
                None
            else: # if not, then false positive
                fp = fp + 1
        else:
            tn = tn + 1
        filter[h_1] = 1
        filter[h_2] = 1
        true_seen.add(user)
    fpr = fp / (fp + tn)
    print('TN', tn)
    print('fp+tn', fp+tn)
    out_fpr.append((i,fpr))

print('FPR DICT')
print(out_fpr)

out_file = 'fpr_t1.csv'
with open(out_file,'w') as out:
    wr = csv.writer(out, delimiter=',')
    wr.writerow(('Time', 'FPR'))
    for elem in out_fpr:
        wr.writerow(elem)





