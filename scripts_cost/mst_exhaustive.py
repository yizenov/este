import os, csv, time, sys
import cost_module

# screen -S mst_exhaustive -dm -L -Logfile screenlog_mst_exhaustive.0 sh -c 'time /usr/bin/python3 scripts_cost/mst_exhaustive.py 0'
# screen -S mst_exhaustive_est -dm -L -Logfile screenlog_mst_exhaustive_est.0 sh -c 'time /usr/bin/python3 scripts_cost/mst_exhaustive.py 1'

print(
"\n \
1. Enter: ~/mst_query_optimization\n \
2. Run the following command: /usr/bin/python3 scripts_cost/mst_exhaustive.py arg1\n \
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

            output_f_file = "output_data/job/costs/exhaustive_opt_plans"
            if cardinality_idx == 0: output_f_file += ".csv"
            elif cardinality_idx == 1: output_f_file += "_psql.csv"

            output_f = open(output_f_file, "w")
            output_f_writer = csv.writer(output_f, delimiter=',')
            output_f_writer.writerow(["query_name", "optimization_time_(ms)", "est_cost", "true_cost", "plan", "physical_plan"])
            output_f.close()

            #############################################################

            class Undirected_Weighted_Graph:
                def __init__(self, query, table_nicks):
                    self.global_plans_set = []
                    self.cost_functions = cost_module.CostFunctions(table_nicks, query, None)

                ############## Exhaustive #########################

                def pruned_exhaustive_enumeration(self, mst_size, edge_list):

                    def insert_edge(arg_comp, arg_idx, arg_edge):
                        if arg_edge not in arg_comp[arg_idx]: arg_comp[arg_idx][arg_edge] = 0
                        arg_comp[arg_idx][arg_edge] += 1

                    def delete_edge(arg_comp, arg_idx, arg_edge):
                        if arg_comp[arg_idx][arg_edge] == 1: arg_comp[arg_idx].pop(arg_edge)
                        else: arg_comp[arg_idx][arg_edge] -= 1

                    glabal_min_cost = [None]
                    best_perm, current_permutation, components = [None, None, None, None], [], []

                    def recursion(arg_best_perm, current_perm, elements, count, mst_size, components, global_min, est_cost, true_cost, all_join_info):
                        if global_min[0] is not None and est_cost >= global_min[0]:
                            return

                        if count == mst_size:
                            self.global_plans_set.append(current_perm[:])

                            curr_plan = "-".join([str(edge) for edge in current_perm])

                            if global_min[0] is None or est_cost < global_min[0]: 
                                arg_best_perm[0], arg_best_perm[1], arg_best_perm[2], arg_best_perm[3] = curr_plan, est_cost, true_cost, all_join_info[:]
                                global_min[0] = est_cost
                            return

                        for idx, i in enumerate(elements):
                            is_cycle = False
                            left, right = False, False
                            left_idx, right_idx = -1, -1
                            for c_idx, comp in enumerate(components):
                                if i[0] in comp and i[1] in comp: 
                                    is_cycle = True
                                    break
                                if i[0] in comp: left, left_idx = True, c_idx
                                if i[1] in comp: right, right_idx = True, c_idx
                            if is_cycle: continue

                            is_bushy, temp_comp = False, None
                            is_new_comp, local_join_info = False, False
                            if not left and not right:  # new component
                                components.append({})
                                components[-1][i[0]] = 1
                                components[-1][i[1]] = 1
                                is_new_comp = True

                                left_component, right_component = [i[0]], [i[1]]
                                edge_weight = self.cost_functions.compute_c_mm_cost(left_component, right_component, None)
                                est_cost += edge_weight[2][cardinality_idx]
                                true_cost += edge_weight[2][0]
                                local_join_info = edge_weight[4][cardinality_idx]
                            elif left and right:  # bushy case
                                left_component, right_component = list(components[left_idx]), list(components[right_idx])
                                edge_weight = self.cost_functions.compute_c_mm_cost(left_component, right_component, None)
                                est_cost += edge_weight[2][cardinality_idx]
                                true_cost += edge_weight[2][0]
                                local_join_info = edge_weight[4][cardinality_idx]

                                insert_edge(components, left_idx, i[0])
                                insert_edge(components, left_idx, i[1])
                                insert_edge(components, right_idx, i[0])
                                insert_edge(components, right_idx, i[1])

                                is_bushy = True
                                temp_comp = components[right_idx]
                                for node in components[right_idx]:
                                    if node not in components[left_idx]: 
                                        components[left_idx][node] = components[right_idx][node]
                                    else: components[left_idx][node] += components[right_idx][node]
                                components.pop(right_idx)
                            elif left:
                                left_component = list(components[left_idx])
                                if i[0] not in components[left_idx]: right_component = [i[0]]
                                else: right_component = [i[1]]
                                edge_weight = self.cost_functions.compute_c_mm_cost(left_component, right_component, None)
                                est_cost += edge_weight[2][cardinality_idx]
                                true_cost += edge_weight[2][0]
                                local_join_info = edge_weight[4][cardinality_idx]

                                insert_edge(components, left_idx, i[0])
                                insert_edge(components, left_idx, i[1])
                            elif right:
                                left_component = list(components[right_idx])
                                if i[0] not in components[right_idx]: right_component = [i[0]]
                                else: right_component = [i[1]]
                                edge_weight = self.cost_functions.compute_c_mm_cost(left_component, right_component, None)
                                est_cost += edge_weight[2][cardinality_idx]
                                true_cost += edge_weight[2][0]
                                local_join_info = edge_weight[4][cardinality_idx]

                                insert_edge(components, right_idx, i[0])
                                insert_edge(components, right_idx, i[1])

                            current_perm.append(i)
                            all_join_info.append(local_join_info)
                            elements.pop(idx)
                            recursion(arg_best_perm, current_perm, elements, count + 1, mst_size, components, global_min, est_cost, true_cost, all_join_info)
                            elements.insert(idx, i)
                            current_perm.pop()
                            all_join_info.pop()

                            if is_new_comp:
                                left_component, right_component = [i[0]], [i[1]]
                                edge_weight = self.cost_functions.compute_c_mm_cost(left_component, right_component, None)
                                est_cost -= edge_weight[2][cardinality_idx]
                                true_cost -= edge_weight[2][0]
                                components.pop()
                            elif is_bushy:
                                components.insert(right_idx, temp_comp)
                                for node in temp_comp:
                                    if components[left_idx][node] == temp_comp[node]: 
                                        components[left_idx].pop(node)
                                    else: components[left_idx][node] -= temp_comp[node]

                                delete_edge(components, left_idx, i[0])
                                delete_edge(components, left_idx, i[1])
                                delete_edge(components, right_idx, i[0])
                                delete_edge(components, right_idx, i[1])

                                left_component, right_component = list(components[left_idx]), list(components[right_idx])
                                edge_weight = self.cost_functions.compute_c_mm_cost(left_component, right_component, None)
                                est_cost -= edge_weight[2][cardinality_idx]
                                true_cost -= edge_weight[2][0]
                            elif left:
                                delete_edge(components, left_idx, i[0])
                                delete_edge(components, left_idx, i[1])

                                left_component = list(components[left_idx])
                                if i[0] not in components[left_idx]: right_component = [i[0]]
                                else: right_component = [i[1]]
                                edge_weight = self.cost_functions.compute_c_mm_cost(left_component, right_component, None)
                                est_cost -= edge_weight[2][cardinality_idx]
                                true_cost -= edge_weight[2][0]
                            elif right:
                                delete_edge(components, right_idx, i[0])
                                delete_edge(components, right_idx, i[1])

                                left_component = list(components[right_idx])
                                if i[0] not in components[right_idx]: right_component = [i[0]]
                                else: right_component = [i[1]]
                                edge_weight = self.cost_functions.compute_c_mm_cost(left_component, right_component, None)
                                est_cost -= edge_weight[2][cardinality_idx]
                                true_cost -= edge_weight[2][0]
                    
                    recursion(best_perm, current_permutation, edge_list, 0, mst_size, components, glabal_min_cost, 0, 0, [])

                    return best_perm[0], best_perm[1], best_perm[2], best_perm[3]

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

                            edge_list = []
                            for join in join_predicates:
                                left, right = join.split(" = ")
                                left, right = left.split(".")[0], right.split(".")[0]
                                edge_list.append((left, right))

                            start_t = time.time()
                            edge_graph = Undirected_Weighted_Graph(query, table_nicks)
                            opt_plan, plan_est_cost, plan_true_cost, mst_physical_plan = edge_graph.pruned_exhaustive_enumeration(len(table_list) - 1, edge_list)
                            end_t = time.time()
                            overhead = round(end_t - start_t, 5)

                            output_f = open(output_f_file, "a")
                            output_f_writer = csv.writer(output_f, delimiter=',')
                            opt_plan = [t[1:-1].split(", ") for t in opt_plan.split("-")]
                            opt_plan = " ".join([l[1:-1] + "-" + r[1:-1] for l, r in opt_plan])
                            mst_physical_plan = " ".join([str(jinf[0] + "-" + jinf[1] + "-" + jinf[2]) for jinf in mst_physical_plan])
                            output_f_writer.writerow([query, overhead, plan_est_cost, plan_true_cost, opt_plan, mst_physical_plan])
                            output_f.close()

            ####################################

            print("Success.\n")
    except:
        print("Wrong parameter type or code error.\n")
