import os, csv, time, sys
import cost_module

# screen -S mst_prim -dm -L -Logfile screenlog_mst_prim.0 sh -c 'time /usr/bin/python3 scripts_cost/mst_prim.py 0'
# screen -S mst_prim_est -dm -L -Logfile screenlog_mst_prim_est.0 sh -c 'time /usr/bin/python3 scripts_cost/mst_prim.py 1'

print(
"\n \
1. Enter: ~/mst_query_optimization\n \
2. Run the following command: /usr/bin/python3 scripts_cost/mst_prim.py arg1\n \
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

            output_f_file = "output_data/job/costs/prim_opt_plans"
            if cardinality_idx == 0: output_f_file += ".csv"
            elif cardinality_idx == 1: output_f_file += "_psql.csv"

            output_f = open(output_f_file, "w")
            output_f_writer = csv.writer(output_f, delimiter=',')
            output_f_writer.writerow(["query_name", "optimization_time_(ms)", "est_cost", "true_cost", "plan", "physical_plan"])
            output_f.close()

            #############################################################

            class Undirected_Weighted_Graph:
                def __init__(self, query, table_nicks):
                    self.graph = {}
                    self.cost_functions = cost_module.CostFunctions(table_nicks, query, None)

                def addEdge(self, u, v):
                    # computing as adjacent edges have redundant steps in cycles
                    if u not in self.graph: self.graph[u] = []
                    if v not in self.graph: self.graph[v] = []
                    self.graph[u].append(v)
                    self.graph[v].append(u)

                ############## Prim #########################

                def primMST(self, min_est_two_way, min_true_two_way, start_edge, two_way_join_info):
                    main_component = [n for n in start_edge]
                    mst_order, mst_est_costs, mst_true_costs, mst_joins_info = [start_edge], [min_est_two_way], [min_true_two_way], [two_way_join_info[cardinality_idx]]
                    adjacent_nodes, adjacent_edges = set(), {}

                    for node in main_component:
                        for adj in self.graph[node]:
                            # NOTE: cyclic/double connection
                            # NOTE: search in main_component is O(n)
                            if adj not in main_component and adj not in adjacent_nodes:
                                adjacent_nodes.add(adj)
                                adjacent_edges[adj] = node

                    while len(main_component) < len(self.graph):
                        est_cost, true_cost, opt_node, join_info = None, None, None, None
                        for adj in adjacent_nodes:
                            edge_weight = self.cost_functions.compute_c_mm_cost(main_component, [adj], None)

                            main_component.append(adj)
                            if est_cost is None or edge_weight[2][cardinality_idx] < est_cost:
                                opt_node, est_cost = adj, edge_weight[2][cardinality_idx]
                                true_cost = edge_weight[2][0]  # NOTE: always compute true cost
                                join_info = edge_weight[4]
                            main_component.pop()
                        
                        mst_order.append((opt_node, adjacent_edges[opt_node]))
                        mst_est_costs.append(est_cost)
                        mst_true_costs.append(true_cost)
                        mst_joins_info.append(join_info[cardinality_idx])

                        main_component.append(opt_node)
                        adjacent_nodes.remove(opt_node)
                        adjacent_edges.pop(opt_node)

                        for adj in self.graph[opt_node]:
                            # NOTE: cyclic/double connection
                            # NOTE: search in main_component is O(n)
                            if adj not in main_component and adj not in adjacent_nodes:
                                adjacent_nodes.add(adj)
                                adjacent_edges[adj] = opt_node

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
                            min_two_way, two_way_true_cost, initial_two_way_join, min_two_way_join_info = None, None, None, None
                            for join in join_predicates:
                                left, right = join.split(" = ")
                                left, right = left.split(".")[0], right.split(".")[0]

                                edge_weight = edge_graph.cost_functions.compute_c_mm_cost([left], [right], None)
                                if min_two_way is None or edge_weight[2][cardinality_idx] < min_two_way:
                                    min_two_way, initial_two_way_join = edge_weight[2][cardinality_idx], (left, right)
                                    two_way_true_cost = edge_weight[2][0]  # NOTE: always compute true cost
                                    min_two_way_join_info = edge_weight[4]
                                
                                # tables may connect on different join attributes
                                edge_graph.addEdge(left, right)

                            mst_plan, mst_est_costs, mst_true_costs, mst_physical_plan = edge_graph.primMST(min_two_way, two_way_true_cost, initial_two_way_join, min_two_way_join_info)

                            mst_est_costs, mst_true_costs = sum(mst_est_costs), sum(mst_true_costs)
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
