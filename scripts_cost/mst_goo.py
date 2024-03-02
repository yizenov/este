import os, csv, time, sys
import cost_module
from heapq import heapify, heappush, heappop

# screen -S mst_goo -dm -L -Logfile screenlog_mst_goo.0 sh -c 'time /usr/bin/python3 scripts_cost/mst_goo.py 0'
# screen -S mst_goo_est -dm -L -Logfile screenlog_mst_goo_est.0 sh -c 'time /usr/bin/python3 scripts_cost/mst_goo.py 1'

print(
"\n \
1. Enter: ~/mst_query_optimization\n \
2. Run the following command: /usr/bin/python3 scripts_cost/mst_goo.py arg1\n \
\t Script requires 1 argument\n \
\t Optimizer: (0 = true cardinality, 1 = PostgreSQL estimated cardinality)\n \
")

print('Number of arguments:', len(sys.argv) - 1)
print('Argument List:', str(sys.argv[1:]), '\n')

if len(sys.argv) != 2:
    print("Wrong number of arguments.\n")
else:
    try:
        cardinality_idx = int(sys.argv[1])

        if cardinality_idx not in [0, 1]: 
            print("Wrong argument types.\n")
        else:

            # job benchmark
            input_queries = "input_data/job/workload_queries"

            output_f_file = "output_data/job/costs/goo_opt_plans"
            if cardinality_idx == 0: output_f_file += ".csv"
            elif cardinality_idx == 1: output_f_file += "_psql.csv"

            output_f = open(output_f_file, "w")
            output_f_writer = csv.writer(output_f, delimiter=',')
            output_f_writer.writerow(["query_name", "optimization_time_(ms)", "est_cost", "true_cost", "plan", "physical_plan"])
            output_f.close()

            #############################################################

            class Undirected_Weighted_Graph:
                def __init__(self, query, table_nicks):
                    self.min_heap, self.components  = [], {}
                    self.cost_functions = cost_module.CostFunctions(table_nicks, query, {})

                def addEdge(self, u, v, w, true_cards, goo_cards, join_info):
                    heappush(self.min_heap, (w, sorted([u, v]), true_cards[:], goo_cards[:], join_info))

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

                            goo_edge = sorted([h_edge[1][0], h_edge[1][1]])
                            self.cost_functions.selectivities.pop((goo_edge[0], goo_edge[1]))
                            break

                def updateMinHeap(self, arg_edge, left_part, right_part):
                    arg_edge = " ".join(sorted(arg_edge))
                    for idx, h_edge in enumerate(self.min_heap):
                        temp_edge = " ".join(h_edge[1])
                        if temp_edge == arg_edge:
                            goo_edge = sorted([h_edge[1][0], h_edge[1][1]])
                            arg_selects = self.cost_functions.selectivities[(goo_edge[0], goo_edge[1])]

                            edge_weight = edge_graph.cost_functions.compute_c_mm_cost(left_part, right_part, arg_selects)

                            self.min_heap[idx] = (edge_weight[3][cardinality_idx], h_edge[1],
                                                edge_weight[2],  # true(true, psql)
                                                edge_weight[3],  # goo(true, psql)
                                                edge_weight[5][cardinality_idx])

                            heapify(self.min_heap)
                            break

                ############## GOO ######################
                
                def find_component(self, root):
                    while self.components[root][0] != root: 
                        root = self.components[root][0]
                    return root
                
                def union_components(self, x, y, left, right):
                    # NOTE: given two components do not overlap
                    if right in self.components[x][1]: self.components[x][1].remove(right)
                    if left in self.components[y][1]: self.components[y][1].remove(left)

                    ######### updating common selectivity in GOO #########
                    goo_edge = sorted([left, right])
                    self.cost_functions.selectivities.pop((goo_edge[0], goo_edge[1]))

                    left_part, right_part = [], []
                    for sel in self.cost_functions.selectivities:
                        if left == sel[0]:
                            left_part.append(sel[1])
                        elif left == sel[1]:
                            left_part.append(sel[0])

                        if right == sel[0]:
                            right_part.append(sel[1])
                        elif right == sel[1]:
                            right_part.append(sel[0])

                    for le in left_part:
                        for re in right_part:
                            if le == re:
                                l_sel, r_sel = -1, -1
                                if (le, left) in self.cost_functions.selectivities:
                                    l_sel = self.cost_functions.selectivities[(le, left)]
                                elif (left, le) in self.cost_functions.selectivities:
                                    l_sel = self.cost_functions.selectivities[(left, le)]

                                if (re, right) in self.cost_functions.selectivities:
                                    r_sel = self.cost_functions.selectivities[(re, right)]
                                elif (right, re) in self.cost_functions.selectivities:
                                    r_sel = self.cost_functions.selectivities[(right, re)]

                                new_sel_true = l_sel[0] * r_sel[0]
                                new_sel_psql = l_sel[1] * r_sel[1]
                                new_sel = [new_sel_true, new_sel_psql]

                                if (le, left) in self.cost_functions.selectivities:
                                    self.cost_functions.selectivities[(le, left)] = new_sel
                                elif (left, le) in self.cost_functions.selectivities:
                                    self.cost_functions.selectivities[(left, le)] = new_sel

                                if (re, right) in self.cost_functions.selectivities:
                                    self.cost_functions.selectivities[(re, right)] = new_sel
                                elif (right, re) in self.cost_functions.selectivities:
                                    self.cost_functions.selectivities[(right, re)] = new_sel
                    ####################################

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

                def gooMST(self):
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
                            mst_est_costs += edge_info[3][cardinality_idx]
                            mst_true_costs += edge_info[2][0]  # NOTE: always compute true cost
                            self.union_components(x, y, edge_info[1][0], edge_info[1][1])
                    return mst_order, mst_est_costs, mst_true_costs, mst_joins_info

            ##################################### Query Complexity ######################################################
            simple_queries, moderate_queries, complex_queries = {}, {}, {}
            for f_idx, file_name in enumerate(sorted(os.listdir(input_queries))):
                query = file_name[2:-4]

                input_query = input_queries + "/" + file_name
                with open(input_query, "r") as query_input_f:

                    original_query = [query_line for query_line in query_input_f]
                    original_query = "".join(original_query).strip()

                    from_and_where = original_query.split('FROM')[1].split('WHERE')
                    table_list = from_and_where[0].split(',')
                    table_list = [table.strip() for table in table_list]
                    table_nicks = {info[1]: info[0] for info in [table.split(" AS ") for table in table_list]}

                    where_clause = from_and_where[1].split('\n\n')
                    where_clause = [clause_set for clause_set in where_clause if clause_set]

                    join_predicates = where_clause[1].split('AND')
                    join_predicates = [join.strip() for join in join_predicates if join.strip()]
                    join_predicates[-1] = join_predicates[-1][:-1]

                    if len(join_predicates) < 10: 
                        if int(query[:-1]) not in simple_queries:
                            simple_queries[int(query[:-1])] = {}
                        if query not in simple_queries[int(query[:-1])]:
                            simple_queries[int(query[:-1])][query] = [len(join_predicates), file_name]
                    elif len(join_predicates) < 20: 
                        if int(query[:-1]) not in moderate_queries:
                            moderate_queries[int(query[:-1])] = {}
                        if query not in moderate_queries[int(query[:-1])]:
                            moderate_queries[int(query[:-1])][query] = [len(join_predicates), file_name]
                    elif len(join_predicates) < 30: 
                        if int(query[:-1]) not in complex_queries:
                            complex_queries[int(query[:-1])] = {}
                        if query not in complex_queries[int(query[:-1])]:
                            complex_queries[int(query[:-1])][query] = [len(join_predicates), file_name]
            
            ############### Original Queries ###########################

            for idx, query_complexity in enumerate([simple_queries, moderate_queries, complex_queries]):
                for query_family in sorted(query_complexity):
                    for query in sorted(query_complexity[query_family]):

                        file_name = query_complexity[query_family][query][1]
                        input_query = input_queries + "/" + file_name
                        with open(input_query, "r") as query_input_f:

                            original_query = [query_line for query_line in query_input_f]
                            original_query = "".join(original_query).strip()

                            from_and_where = original_query.split('FROM')[1].split('WHERE')
                            table_list = from_and_where[0].split(',')
                            table_list = [table.strip() for table in table_list]
                            table_nicks = {info[1]: info[0] for info in [table.split(" AS ") for table in table_list]}

                            where_clause = from_and_where[1].split('\n\n')
                            where_clause = [clause_set for clause_set in where_clause if clause_set]

                            join_predicates = where_clause[1].split('AND')
                            join_predicates = [join.strip() for join in join_predicates if join.strip()]
                            join_predicates[-1] = join_predicates[-1][:-1]

                            start_t = time.time()
                            edge_graph = Undirected_Weighted_Graph(query, table_nicks)
                            for join in join_predicates:
                                left, right = join.split(" = ")
                                left, right = left.split(".")[0], right.split(".")[0]

                                cardinalities = edge_graph.cost_functions.all_cardinalities[query]

                                l_true_size = cardinalities[left][2]
                                r_true_size = cardinalities[right][2]
                                l_est_size = cardinalities[left][3]
                                r_est_size = cardinalities[right][3]

                                # NOTE: GOO
                                edge_graph.cost_functions.goo_cardinalities[left] = [l_true_size, l_est_size]
                                edge_graph.cost_functions.goo_cardinalities[right] = [r_true_size, r_est_size]

                                two_way = " ".join(sorted([left, right]))
                                two_way_card_true = cardinalities[two_way][2]
                                two_way_card_psql = cardinalities[two_way][3]

                                selectivity_true = two_way_card_true / (l_true_size * r_true_size)
                                selectivity_psql = two_way_card_psql / (l_est_size * r_est_size)
                                all_selects = [selectivity_true, selectivity_psql]

                                init_sel = sorted([left, right])
                                edge_graph.cost_functions.selectivities[(init_sel[0], init_sel[1])] = all_selects

                                edge_weight = edge_graph.cost_functions.compute_c_mm_cost([left], [right], all_selects)

                                # tables may connect on different join attributes
                                edge_graph.addEdge(left, right, edge_weight[2][cardinality_idx], edge_weight[2], edge_weight[3], edge_weight[5][cardinality_idx])

                            mst_plan, mst_est_costs, mst_true_costs, mst_physical_plan = edge_graph.gooMST()
                            end_t = time.time()
                            overhead = round(end_t - start_t, 5)

                            output_f = open(output_f_file, "a")
                            output_f_writer = csv.writer(output_f, delimiter=',')
                            mst_plan = " ".join([str(edge[0] + "-" + edge[1]) for edge in mst_plan])
                            mst_physical_plan = " ".join([str(jinf[0] + "-" + jinf[1] + "-" + jinf[2]) for jinf in mst_physical_plan])
                            output_f_writer.writerow([query, overhead, mst_est_costs, mst_true_costs, mst_plan, mst_physical_plan])
                            output_f.close()

            ####################################

            print("Success.\n")
    except:
        print("Wrong parameter type or code error.\n")
