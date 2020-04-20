from pyspark import SparkContext
import os,csv
from operator import itemgetter
import itertools,sys
import collections
import matplotlib.pyplot as plt


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


# === Perform Betweenness Calculation === #
### ==== Start from root and work way down tree to get shortest paths at each node step 2 GNA === #
def get_shortest_paths(lvels,max_level):
    total_shortest = {lvels[0][0]: 1}                  # initialize the root node
    for level in range(1,max_level+1):                # iterate over levels starting from root + 1
        for child in lvels[level]:                   # iterate over each node in current level
            for parent in (set(graph_dict[child]) & set(lvels[level-1])):            # iterate over nodes parents
                # if the parent shares a dag edge in the above level
                if child not in total_shortest:
                    total_shortest[child] = total_shortest[parent]  # initalize child node with first seen parent
                    # assumption here is that children are in above level
                else:
                    total_shortest[child] = total_shortest[parent]  +total_shortest[child]
    return total_shortest


def GNA_betweenness(root):
    stack = [root]  # starting traversal down tree
    nodes = set(stack)  # Store nodes traversed
    nodes_level = {root: 0}  # track depth level of nodes
    bfs_child_parents = {}  # store parents of nodes  - sigma _st in paper
    ind = 0
    total_shortest = {root: 1}
    # first sweep in shortest path alg
    while ind < len(stack):
        head = stack[ind]  # get parent
        children = graph_dict[head]  # get children
        for child in children:
            if child not in nodes:  # if we haven't visited node yet
                stack.append(child)  # add to stack
                nodes.add(child)
                nodes_level[child] = nodes_level[head] + 1
                bfs_child_parents[child] = [head]
            else:
                if nodes_level[child] == nodes_level[head] + 1:
                    bfs_child_parents[child].append(head)
        ind = ind + 1

    # === Get total number of shortest paths GNA s2 === #
    lvels = {}
    for k, v in nodes_level.items():
        lvels.setdefault(v, []).append(k)
    total_shortest = get_shortest_paths(lvels, max(list(nodes_level.values())))

    # === second sweep in shorteset path alg -- go through bfs tree in REVERSE order === #
    node_credits = {}  # node_credits for
    for i in range(len(stack) - 1, -1, -1):
        node = stack[i]
        if node not in node_credits:
            node_credits[node] = 1
        if node in bfs_child_parents:
            parents = bfs_child_parents[node]  # get this nodes parents to spread credits

            check = 0
            for parent in parents:
                if parent not in node_credits and parent != root:
                    node_credits[parent] = 1
                if parent not in node_credits and parent == root:
                    node_credits[parent] = 0
                # spread credits basesd off parents number of shortest paths
                # total number of shortest paths from 'nodes' parents to distribute credits to parents fairly
                spread = node_credits[node] * (total_shortest[parent] / total_shortest[node])
                check = check + spread
                node_credits[parent] = node_credits[parent] + spread
                edge = tuple(sorted([parent, node]))

                yield (edge, spread / 2)
                # we yield since each bfs will have a contribution to the edges overall betweenness sum (p.2 of ref)


def single_shortest_path(G,node):
    in_path = set()
    temp_path = {node}
    while temp_path:
        current_path = temp_path
        temp_path = set()
        for node in current_path:
            if node not in in_path:
                yield node
                in_path.add(node)
                [temp_path.add(x) for x in G[node]] # nodes neighbors to go traverse over


def connected_components(G):
    seen = set()
    for node in G:
        if node not in seen:
            c = set(single_shortest_path(G,node)) # get all nodes that is traversable from this node
            yield c
            [seen.add(x) for x in c]



def calc_modularity(S):
    group_sum = 0
    for s in S:
        for i in s:
            for j in s:
                if i == j: a_ij = 0
                else:      a_ij = (j in ORIGINAL_GRAPH[i])
                s_um = a_ij - ((len(ORIGINAL_GRAPH[i])*len(ORIGINAL_GRAPH[j]))/(2*m))
                group_sum = group_sum + s_um
    return group_sum/(2*m)

tic()
# ==== Load Data ==== #
input_file_path = 'power_input.txt'
#input_file_path = 'GNA_TEST.txt'
#input_file_path = source[1]
sc = SparkContext('local[*]', 'task1')
edges = sc.textFile(input_file_path).map(lambda x: tuple(x.split(' ')))
# === Make a Dict of the Graph {node : [ children ] } === #
parent_child = edges.flatMap(lambda x: [ (x[0], [x[1]]) , (x[1], [x[0]]) ]).reduceByKey(lambda a,b: a + b).mapValues(set)
#####      (parent, [list of children])
graph_dict = parent_child.collectAsMap()
ORIGINAL_GRAPH = parent_child.collectAsMap()
# refrenced https://sites.cs.ucsb.edu/~gilbert/cs240a/old/cs240aSpr2011/hw4/hw4.pdf
# reduce by key to remedy issue of updating betweenness value for an edge for multiple BFS
out_file = 'task2_btwn.txt'
#out_file = source[2]
m = edges.count()
q = {}
c = 0
vertices = edges.flatMap(lambda x: [x[0], x[1]]).distinct()
ma_x = 0
best_dict = []
while True:
    betweenness = vertices.flatMap(lambda x: GNA_betweenness(x) ).reduceByKey(lambda a, b: a + b).sortBy(lambda x: (-x[1]))

    # === Write OG Graph betweenness values === #
    if c == 0:
        calcs = betweenness.collect()
        with open(out_file, 'w') as out:
            for line in calcs:
                line = '('+ str(line[0])[1:-1] +')' + ', ' + str(line[1])
                out.write(line)
                out.write('\n')
        c = 1
    # === Remove Max === #
    max_betweenness = betweenness.first()
    graph_dict[max_betweenness[0][0]].remove(max_betweenness[0][1])
    graph_dict[max_betweenness[0][1]].remove(max_betweenness[0][0])

    # === Calculate modularity === #
    final_communities = list(connected_components(graph_dict))
    q[len(final_communities)] = calc_modularity(final_communities)

    # store best community components
    if q[len(final_communities)] > ma_x:
        ma_x = q[len(final_communities)]
        best_dict = [sorted(x) for x in final_communities]

    if len(final_communities) == len(list(graph_dict.keys())):
        print('breaking')
        break

#plt.plot(list(q.keys()),list(q.values()))
#plt.show()
print(toc())
out_file2 = 'communities.txt'
#out_file2 = source[3]
print('Max number communities   ',len(best_dict))
#best_dict.sort(key=lambda x: (len(x),x))
best_dict = sorted(best_dict, key = lambda x: (len(x),str(x)[1:-1]))
with open(out_file2,'w') as out:
    for line in best_dict:
        out.write(str(line)[1:-1])
        out.write('\n')
