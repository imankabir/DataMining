from blackbox import BlackBox
import binascii
import random,csv, itertools,sys

global seqnum
stream_size = 100
num_of_asks = 30
#stream_size = int(sys.argv[2])
#num_of_asks = int(sys.argv[3])


def main(*source):
    random.seed(553)
    input_file = 'users.txt'
    #input_file = source[0]
    bx = BlackBox()
    sample_size = 100
    sample = []
    n = 101
    replaces = 0
    seqnum = 100
    output = []
    for i in range(0,num_of_asks):
        stream_users = bx.ask(input_file,stream_size)
        # === Initialize the Sample === #
        if i == 0:
            [sample.append(x) for x in stream_users]
            out = (seqnum, sample[0], sample[20], sample[40], sample[60], sample[80])
            output.append(out)
            seqnum = seqnum + 100
            continue
        else:
            for user in stream_users:
                prob_keep = len(sample)
                actual_keep = random.randint(0,100000)

                if (prob_keep)  > (actual_keep % n):
                    replaces = replaces + 1
                    rm = random.randint(0,100000) % 100
                    sample[rm] = user
                n = n + 1
            out = (seqnum, sample[0], sample[20], sample[40], sample[60], sample[80])
            output.append(out)
            seqnum = seqnum + 100

    print(replaces)

    out_file = 't3.csv'
    #out_file = source[1]
    with open(out_file,'w') as out:
        wr = csv.writer(out, delimiter=',')
        wr.writerow(('seqnum', '0_id','20_id','40_id','60_id','80_id'))
        for elem in output:
            wr.writerow(elem)

#if __name__ == '__main__':
    #main(sys.argv[1],sys.argv[-1])
main()