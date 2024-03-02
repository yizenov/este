import os, csv, sys, statistics

print(
"\n \
1. Enter: ~/mst_query_optimization\n \
2. Run the following command: /usr/bin/python3 scripts_runtime/parse_runtime.py arg1\n \
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
            runs_per_query = 5

            enumerations = ["exhaustive", "goo", "kruskal_ensemble", "kruskal", "prim_ensemble", "prim"]

            if cardinality_idx == 1: optimality_type = "EST"
            else: optimality_type = "TRUE"
            results_folder = "output_data/job/PHYSICAL_PLANS/" + optimality_type + "_PLANS/"

            output_f_csv_file = "output_data/job/runtime/enum_run_job_"

            ##################################### Query Runtime #########################################################

            all_runtime_data = []
            for enum_idx, enum_method in enumerate(enumerations):
                all_runtime_data.append({})

                for idx, file_name in enumerate(sorted(os.listdir(results_folder + enum_method))):
                    if ".result" not in file_name: continue
                    query_name = file_name.split(".")[0].split("_")[2]

                    result_file = results_folder + enum_method + "/" + file_name
                    with open(result_file, "r") as input_f:

                        opt_runtimes, exec_runtimes, total_runtimes = [], [], []
                        for idx, line in enumerate(input_f):
                            if "Planning Time: " in line:  # in ms
                                opt_runtimes.append(float(line.split(":")[1].split(" ms")[0].strip()))
                            elif "Execution Time: " in line: 
                                exec_runtimes.append(float(line.split(":")[1].split(" ms")[0].strip()))
                            elif "Time: " in line: 
                                total_runtimes.append(float(line.split(":")[1].split(" ms")[0].strip()))

                        if len(opt_runtimes) != runs_per_query or \
                            len(exec_runtimes) != runs_per_query or \
                            len(total_runtimes) != runs_per_query: 
                                print(["missing runs, ", enum_method, cardinality_idx, file_name])
                                continue

                        opt_runtimes = statistics.median(opt_runtimes)
                        exec_runtimes = statistics.median(exec_runtimes)
                        total_runtimes = statistics.median(total_runtimes)

                        all_runtime_data[enum_idx][query_name] = [opt_runtimes, exec_runtimes, total_runtimes]

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

            ##################################### Results ######################################################
            for enum_idx, enum_method in enumerate(enumerations):

                temp_file_name = output_f_csv_file + enum_method
                if cardinality_idx == 1: temp_file_name += "_psql.csv"
                else: temp_file_name += ".csv"

                output_f = open(temp_file_name, "w")
                output_f_writer = csv.writer(output_f, delimiter=',')
                output_f_writer.writerow(["query", "psql opt (ms)", "psql exec (ms)", "psql total (ms)"])

                for idx, query_complexity in enumerate([simple_queries, moderate_queries, complex_queries]):
                    for query_family in sorted(query_complexity):
                        for query in sorted(query_complexity[query_family]):

                            if query in all_runtime_data[enum_idx]: runs = all_runtime_data[enum_idx][query]
                            else: 
                                print(["missing runtime time", enum_method, cardinality_idx, query])
                                runs = [-1, -1, -1]
                                continue

                            output_f_writer.writerow([query, runs[0], runs[1], runs[2]])

                output_f.close()

            ####################################

            print("\nSuccess.\n")
    except:
        print("Script errors.\n")
