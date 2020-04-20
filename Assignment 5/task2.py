from blackbox import BlackBox
import binascii
import random,csv, itertools,sys,collections

stream_size = 300
num_of_asks = 30
#stream_size = int(sys.argv[2])
#num_of_asks = int(sys.argv[3])


input_file = 'users.txt'
#input_file = sys.argv[1]
bx = BlackBox()

def hash_funcs(k):
    sample = range(0,1000,13)
    a = random.sample(sample,10)
    b = random.sample(sample, 10)
    p = [757]
    funcs = list(itertools.product(a,b,p))[0:k]
    return funcs


def myhash(s):
    result = []
    hash_function_list = [311,821,2,3]
    for f in hash_function_list:
        result.append((f*s + 757)% 7039)
    return result

#k = 50
#k_hashes = hash_funcs(k)
k_hashes = [(311, 757, 7039), (821, 757, 7039), (2, 757, 7039),(3, 757, 7039)]
k = len(k_hashes)
final_counts = []
estimations = []
ground_truth = []
for i in range(0,num_of_asks):
    stream_users = bx.ask(input_file,stream_size)
    # === Convert user_id to int === #
    stream_users= [int(binascii.hexlify(u.encode('utf8')),16) for u in stream_users]
    for func in k_hashes:
        bit_strs = []
        for user,kk in zip(stream_users,range(0,100)):
            bitstr = bin((func[0]*user + func[1]) % func[2]).split('b')[1]
            bit_strs.append(bitstr)
        longest = 0
        for bitstr in bit_strs:
            reverse = bitstr[::-1]
            one_pos = reverse.find('1')
            if one_pos > longest:
                longest = one_pos

        final_counts.append(2**longest)
    actual_unique = dict(collections.Counter(stream_users)).keys()
    ground_truth.append(len(actual_unique))
    num_splits = 2
    split = k // num_splits
    means = []
    #print(final_counts)
    #final_counts.sort()
    split_data1 = final_counts[:split]
    split_data2 = final_counts[split:]
    mean = [sum(split_data1) / len(split_data1), sum(split_data2) / len(split_data2)]
    median = sum(mean) / 2
    print('Appx: ', int(median))
    estimations.append((i,len(actual_unique),int(median)))
print(ground_truth)
print('Overall Error: ', sum([i[1] for i in estimations])/sum(ground_truth))

out_file = 'ground_truth_t2.csv'
#out_file = sys.argv[4]
with open(out_file,'w') as out:
    wr = csv.writer(out, delimiter=',')
    wr.writerow(('Time','Ground Truth','Estimation'))
    for elem in estimations:
        wr.writerow(elem)
    wr.writerow(())