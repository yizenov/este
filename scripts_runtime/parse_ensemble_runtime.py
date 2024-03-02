import os, csv, sys

print(
"\n \
1. Enter: ~/mst_query_optimization\n \
2. Run the following command: /usr/bin/python3 scripts_runtime/parse_ensemble_runtime.py arg1\n \
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

            enumeration_ensemble = ["kruskal_ensemble", "prim_ensemble"]

            query_cost_plans_file = "output_data/job/costs/"
            query_runtimes_file = "output_data/job/runtime/enum_run_job_"

            output_run_csv_file = "output_data/job/runtime/enum_run_job_ensemble"
            if cardinality_idx == 1: output_run_csv_file += "_psql.csv"
            else: output_run_csv_file += ".csv"

            output_run_f = open(output_run_csv_file, "w")
            output_run_f_writer = csv.writer(output_run_f, delimiter=',')
            output_run_f_writer.writerow(["query", "psql opt (ms)", "psql exec (ms)", "psql total (ms)"])

            ############################ Query Cost and Runtime #########################################################

            all_cost_data, all_runtime_data = [{}, {}], [{}, {}]
            for mst_idx, mst_type in enumerate(enumeration_ensemble):

                temp_cost_file_name = query_cost_plans_file + mst_type + "_opt_plans"
                if cardinality_idx == 1: temp_cost_file_name += "_psql.csv"
                else: temp_cost_file_name += ".csv"

                with open(temp_cost_file_name, "r") as input_f:
                    for idx, line in enumerate(input_f):
                        if idx == 0: continue
                        line = line.strip().split(',')

                        query, enum_time = line[0], float(line[1])
                        est_cost, true_cost = float(line[2]), float(line[3])

                        if query not in all_cost_data[mst_idx]:
                            all_cost_data[mst_idx][query] = [query, enum_time, 
                                                            est_cost, true_cost,
                                                            line[4].strip(), line[5].strip()]
                        else: print("duplicate")

                temp_runtime_file_name = query_runtimes_file + mst_type
                if cardinality_idx == 1: temp_runtime_file_name += "_psql.csv"
                else: temp_runtime_file_name += ".csv"

                with open(temp_runtime_file_name, "r") as input_f:
                    for idx, line in enumerate(input_f):
                        if idx == 0: continue
                        line = line.strip().split(',')

                        query, psql_opt_time = line[0], float(line[1])
                        psql_exec_time, psql_total_time = float(line[2]), float(line[3])

                        if query not in all_runtime_data[mst_idx]:
                            all_runtime_data[mst_idx][query] = [query, psql_opt_time, 
                                                    psql_exec_time, psql_total_time]
                        else: print("duplicate")

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
                            simple_queries[int(query[:-1])][query] = len(join_predicates)
                    elif len(join_predicates) < 20: 
                        if int(query[:-1]) not in moderate_queries:
                            moderate_queries[int(query[:-1])] = {}
                        if query not in moderate_queries[int(query[:-1])]:
                            moderate_queries[int(query[:-1])][query] = len(join_predicates)
                    elif len(join_predicates) < 30: 
                        if int(query[:-1]) not in complex_queries:
                            complex_queries[int(query[:-1])] = {}
                        if query not in complex_queries[int(query[:-1])]:
                            complex_queries[int(query[:-1])][query] = len(join_predicates)

            ##################################### Build Ensemble ########################################################

            for idx, query_complexity in enumerate([simple_queries, moderate_queries, complex_queries]):
                for query_family in sorted(query_complexity):
                    for query in sorted(query_complexity[query_family]):

                        if query not in all_cost_data[0] or query not in all_cost_data[1]:
                            print(["missing enum cost", cardinality_idx, query])
                            continue
                        if query not in all_runtime_data[0] or query not in all_runtime_data[1]:
                            print(["missing enum runtime", cardinality_idx, query])
                            continue

                        kruskal_cost, prim_cost = all_cost_data[0][query], all_cost_data[1][query]
                        kruskal_time, prim_time = all_runtime_data[0][query], all_runtime_data[1][query]

                        if cardinality_idx == 1: card_type = 2
                        else: card_type = 3

                        if prim_cost[card_type] < kruskal_cost[card_type]: 
                            output_run_f_writer.writerow(prim_time)
                        else: 
                            output_run_f_writer.writerow(kruskal_time)

            output_run_f.close()

            ####################################

            print("\nSuccess.\n")
    except:
        print("Script errors.\n")
