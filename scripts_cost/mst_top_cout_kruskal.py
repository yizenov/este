import os, csv, time, sys
import cost_module_top_cout
from heapq import heapify, heappush, heappop

# screen -S mst_top_cout_kruskal -dm -L -Logfile screenlog_mst_top_cout_kruskal.0 sh -c 'time /usr/bin/python3 scripts_cost/mst_top_cout_kruskal.py'

print(
"\n \
1. Enter: ~/mst_query_optimization\n \
2. Run the following command: /usr/bin/python3 scripts_cost/mst_top_cout_kruskal.py\n \
\t Script requires 0 arguments\n \
")

print('Number of arguments:', len(sys.argv) - 1)
print('Argument List:', str(sys.argv[1:]), '\n')

if len(sys.argv) != 1:
    print("Wrong number of arguments.\n")
else:
    try:
        cardinality_idx = 0 # NOTE: always for this workload

        if cardinality_idx not in [0, 1]: 
            print("Wrong argument types.\n")
        else:
            input_queries = "input_data/topology/workload_queries"

            output_f_file = "output_data/topology/costs_cout/kruskal_opt_plans.csv"
            output_f = open(output_f_file, "w")
            output_f_writer = csv.writer(output_f, delimiter=',')
            output_f_writer.writerow(["query_name", "optimization_time_(ms)", "est_cost", "true_cost", "plan", "physical_plan"])
            output_f.close()

            all_cardinalities = cost_module_top_cout.load_cardinalities()

            #############################################################

            class Undirected_Weighted_Graph:
                def __init__(self, query, table_nicks, arg_all_cardinalities):
                    self.min_heap, self.components  = [], {}
                    self.cost_functions = cost_module_top_cout.CostFunctionsTopCout(table_nicks, query, None, arg_all_cardinalities)
                    self.query = query

                def addEdge(self, u, v, w, cards, join_info):
                    heappush(self.min_heap, (w, sorted([u, v]), cards[0], cards[1], join_info))

                    # computing as adjacent edges have redundant steps in cycles
                    if u not in self.components: self.components[u] = (u, set(), [u])
                    if v not in self.components: self.components[v] = (v, set(), [v])
                    self.components[u][1].add(v)
                    self.components[v][1].add(u)

                def removeFromMinHeap(self, arg_edge):
                    # NOTE: double edge case e.g. t.id joins with two tables on the same attribute
                    if arg_edge[1] in self.components[arg_edge[0]][1]:
                        self.components[arg_edge[0]][1].remove(arg_edge[1])
                    if arg_edge[0] in self.components[arg_edge[1]][1]:
                        self.components[arg_edge[1]][1].remove(arg_edge[0])

                    # edges to be deleted from min_heap
                    arg_edge = " ".join(sorted(arg_edge))
                    for idx, h_edge in enumerate(self.min_heap):
                        temp_edge = " ".join(h_edge[1])
                        if temp_edge == arg_edge:
                            self.min_heap.pop(idx)
                            heapify(self.min_heap)
                            break

                def updateMinHeap(self, arg_edge, left_part, right_part):
                    arg_edge = " ".join(sorted(arg_edge))
                    for idx, h_edge in enumerate(self.min_heap):
                        temp_edge = " ".join(h_edge[1])
                        if temp_edge == arg_edge:
                            edge_weight = self.cost_functions.compute_c_mm_cost(left_part, right_part, None)
                            self.min_heap[idx] = (edge_weight[2][cardinality_idx], h_edge[1],
                                                edge_weight[2][0], edge_weight[2][1], edge_weight[4][cardinality_idx])  # true, psql
                            heapify(self.min_heap)
                            break

                ############## Kruskal ######################
                
                def find_component(self, root):
                    while self.components[root][0] != root: 
                        root = self.components[root][0]
                    return root
                
                def union_components(self, x, y, left, right):
                    # NOTE: given two components do not overlap
                    if right in self.components[x][1]: self.components[x][1].remove(right)
                    if left in self.components[y][1]: self.components[y][1].remove(left)

                    # NOTE: REMOVE: multi-connections between given two components
                    left_delete_nodes = [n for n in self.components[x][1] if n in self.components[y][2]]
                    right_delete_nodes = [n for n in self.components[y][1] if n in self.components[x][2]]

                    for n in left_delete_nodes:
                        if n in self.components[x][1]: self.components[x][1].remove(n)
                        self.removeFromMinHeap([x, n])
                    for n in right_delete_nodes:
                        if n in self.components[y][1]: self.components[y][1].remove(n)
                        self.removeFromMinHeap([y, n])

                    # NOTE: REMOVE untraversed common nodes for the next expansion
                    common_delete_nodes = []
                    for n in self.components[y][1]:
                        if n in self.components[x][1]: common_delete_nodes.append(n)
                        else: self.components[x][1].add(n)
                    for n in common_delete_nodes:
                        self.components[y][1].remove(n)
                        self.removeFromMinHeap([y, n])
                    
                    # NOTE: merging already traversed nodes (common component)
                    [self.components[x][2].append(n) for n in self.components[y][2]]
                    self.components[y] = (x, set(), [])

                    # NOTE: update costs with untraversed nodes in min-heap
                    for untraversed_adj_node in self.components[x][1]:
                        adj_node_component = self.find_component(untraversed_adj_node)
                        for in_node in self.components[adj_node_component][2]:
                            for out_node in self.components[x][2]:
                                # NOTE: update all multi-connections between the two components
                                self.updateMinHeap([out_node, in_node], 
                                                self.components[x][2], 
                                                self.components[adj_node_component][2])

                def kruskalMST(self):
                    mst_order, mst_joins_info, mst_length = [], [], len(self.components) - 1
                    mst_est_costs, mst_true_costs = 0, 0
                    while len(mst_order) < mst_length:
                        edge_info = heappop(self.min_heap)

                        x = self.find_component(edge_info[1][0])
                        y = self.find_component(edge_info[1][1])

                        # Discard the edge if both are in the same component
                        if x != y:
                            mst_order.append([edge_info[1][0], edge_info[1][1]])
                            mst_joins_info.append(edge_info[4])
                            mst_est_costs += edge_info[0]
                            mst_true_costs += edge_info[2]  # NOTE: always compute true cost
                            self.union_components(x, y, edge_info[1][0], edge_info[1][1])
                    return mst_order, mst_est_costs, mst_true_costs, mst_joins_info

            ############### Original Queries ###########################

            for f_idx, file_name in enumerate(sorted(os.listdir(input_queries))):
                query = file_name[:-4]

                input_query = input_queries + "/" + file_name
                with open(input_query, "r") as query_input_f:

                    original_query = [query_line for query_line in query_input_f]
                    original_query = "".join(original_query).strip()

                    from_and_where = original_query.split('FROM')[1].split('WHERE')
                    table_list = from_and_where[0].split(',')
                    table_list = [table.strip() for table in table_list]
                    table_nicks = {info[1]: info[0] for info in [table.split(" AS ") for table in table_list]}

                    join_predicates = from_and_where[1].split('AND')
                    join_predicates = [join.strip() for join in join_predicates if join.strip()]
                    join_predicates[-1] = join_predicates[-1][:-1]

                    start_t = time.time()
                    edge_graph = Undirected_Weighted_Graph(query, table_nicks, all_cardinalities)
                    for join in join_predicates:
                        left, right = join.split(" = ")
                        left, right = left.split(".")[0], right.split(".")[0]

                        edge_weight = edge_graph.cost_functions.compute_c_mm_cost([left], [right], None)

                        # tables may connect on different join attributes
                        edge_graph.addEdge(left, right, edge_weight[2][cardinality_idx], edge_weight[2], edge_weight[4][cardinality_idx])

                    mst_plan, mst_est_costs, mst_true_costs, mst_physical_plan = edge_graph.kruskalMST()
                    end_t = time.time()
                    overhead = round(end_t - start_t, 5)

                    output_f = open(output_f_file, "a")
                    output_f_writer = csv.writer(output_f, delimiter=',')
                    mst_plan = " ".join([str(edge[0] + "-" + edge[1]) for edge in mst_plan])
                    output_f_writer.writerow([query, overhead, mst_est_costs, mst_true_costs, mst_plan, None])
                    output_f.close()

            ####################################

            print("Success.\n")
    except:
        print("Wrong parameter type or code error.\n")
