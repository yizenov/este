import os, csv, sys

print(
"\n \
1. Enter: ~/mst_query_optimization\n \
2. Run the following command: /usr/bin/python3 figures/generate_runtime_figure_data.py\n \
\t Script requires 0 argument\n \
")

print('Number of arguments:', len(sys.argv) - 1)
print('Argument List:', str(sys.argv[1:]), '\n')

if len(sys.argv) != 1:
    print("Wrong number of arguments.\n")
else:
    try:

        # job benchmark
        input_queries = "input_data/job/workload_queries"

        sec_time = 1000

        # true data
        true_prim_data_file = "output_data/job/runtime/enum_run_job_prim.csv"
        true_prim_ensemble_data_file = "output_data/job/runtime/enum_run_job_prim_ensemble.csv"
        true_kruskal_data_file = "output_data/job/runtime/enum_run_job_kruskal.csv"
        true_kruskal_ensemble_data_file = "output_data/job/runtime/enum_run_job_kruskal_ensemble.csv"
        true_goo_data_file = "output_data/job/runtime/enum_run_job_goo.csv"
        true_ensemble_data_file = "output_data/job/runtime/enum_run_job_ensemble.csv"
        true_exhaustive_data_file = "output_data/job/runtime/enum_run_job_exhaustive.csv"

        # estimated data
        est_prim_data_file = "output_data/job/runtime/enum_run_job_prim_psql.csv"
        est_prim_ensemble_data_file = "output_data/job/runtime/enum_run_job_prim_ensemble_psql.csv"
        est_kruskal_data_file = "output_data/job/runtime/enum_run_job_kruskal_psql.csv"
        est_kruskal_ensemble_data_file = "output_data/job/runtime/enum_run_job_kruskal_ensemble_psql.csv"
        est_goo_data_file = "output_data/job/runtime/enum_run_job_goo_psql.csv"
        est_ensemble_data_file = "output_data/job/runtime/enum_run_job_ensemble_psql.csv"
        est_exhaustive_data_file = "output_data/job/runtime/enum_run_job_exhaustive_psql.csv"

        figure_9a_file = "figures/figures_data/figure_9a_data.csv"  # true runtime
        tables_1_exec_file = "figures/figures_data/tables_1_exec_data.csv"  # true runtime table
        figure_10a_file = "figures/figures_data/figure_10a_data.csv"  # true psql opt time
        tables_1_psql_opt_file = "figures/figures_data/tables_1_psql_opt_data.csv"  # true psql opt time table

        figure_9b_file = "figures/figures_data/figure_9b_data.csv"  # est runtime
        tables_2_exec_file = "figures/figures_data/tables_2_exec_data.csv"  # est runtime table
        figure_10b_file = "figures/figures_data/figure_10b_data.csv"  # est psql opt time
        tables_2_psql_opt_file = "figures/figures_data/tables_2_psql_opt_data.csv"  # est psql opt time table

        class GenerateFigures(object):
            def __init__(self):

                self.simple_queries = {}
                self.moderate_queries = {}
                self.complex_queries = {}

                self.true_prim_data, self.true_prim_ensemble_data = {}, {}
                self.true_kruskal_data, self.true_kruskal_ensemble_data = {}, {}
                self.true_ensemble_data, self.true_goo_data, self.true_exhaustive_data = {}, {}, {}

                self.est_prim_data, self.est_prim_ensemble_data = {}, {}
                self.est_kruskal_data, self.est_kruskal_ensemble_data = {}, {}
                self.est_ensemble_data, self.est_goo_data, self.est_exhaustive_data = {}, {}, {}

                self.load_queries()
                self.load_true_data()
                self.load_estimated_data()
                self.generate_figure_true_data()
                self.generate_figure_estimated_data()

            def load_queries(self):
                for file_name in sorted(os.listdir(input_queries)):
                    query = file_name[2:-4]

                    input_query = input_queries + "/" + file_name
                    with open(input_query, "r") as query_input_f:

                        original_query = [query_line for query_line in query_input_f]
                        original_query = "".join(original_query).strip()

                        from_and_where = original_query.split('FROM')[1].split('WHERE')
                        table_list = from_and_where[0].split(',')
                        table_list = [table.strip() for table in table_list]

                        where_clause = from_and_where[1].split('\n\n')
                        where_clause = [clause_set for clause_set in where_clause if clause_set]

                        join_predicates = where_clause[1].split('AND')
                        join_predicates = [join.strip() for join in join_predicates if join.strip()]
                        join_predicates[-1] = join_predicates[-1][:-1]

                        if len(join_predicates) < 10: 
                            if int(query[:-1]) not in self.simple_queries:
                                self.simple_queries[int(query[:-1])] = {}
                            if query not in self.simple_queries[int(query[:-1])]:
                                self.simple_queries[int(query[:-1])][query] = len(join_predicates)
                        elif len(join_predicates) < 20: 
                            if int(query[:-1]) not in self.moderate_queries:
                                self.moderate_queries[int(query[:-1])] = {}
                            if query not in self.moderate_queries[int(query[:-1])]:
                                self.moderate_queries[int(query[:-1])][query] = len(join_predicates)
                        elif len(join_predicates) < 30: 
                            if int(query[:-1]) not in self.complex_queries:
                                self.complex_queries[int(query[:-1])] = {}
                            if query not in self.complex_queries[int(query[:-1])]:
                                self.complex_queries[int(query[:-1])][query] = len(join_predicates)

            def load_true_data(self):
                with open(true_prim_data_file, "r") as input_f:
                    for idx, line in enumerate(input_f):
                        if idx == 0: continue
                        line = line.strip().split(",")

                        query, psql_opt_time = line[0].strip(), float(line[1].strip())
                        exec_time, total_time = float(line[2].strip()), float(line[3].strip())

                        self.true_prim_data[query] = [psql_opt_time, exec_time, total_time]

                with open(true_prim_ensemble_data_file, "r") as input_f:
                    for idx, line in enumerate(input_f):
                        if idx == 0: continue
                        line = line.strip().split(",")

                        query, psql_opt_time = line[0].strip(), float(line[1].strip())
                        exec_time, total_time = float(line[2].strip()), float(line[3].strip())

                        self.true_prim_ensemble_data[query] = [psql_opt_time, exec_time, total_time]

                with open(true_kruskal_data_file, "r") as input_f:
                    for idx, line in enumerate(input_f):
                        if idx == 0: continue
                        line = line.strip().split(",")

                        query, psql_opt_time = line[0].strip(), float(line[1].strip())
                        exec_time, total_time = float(line[2].strip()), float(line[3].strip())

                        self.true_kruskal_data[query] = [psql_opt_time, exec_time, total_time]

                with open(true_kruskal_ensemble_data_file, "r") as input_f:
                    for idx, line in enumerate(input_f):
                        if idx == 0: continue
                        line = line.strip().split(",")

                        query, psql_opt_time = line[0].strip(), float(line[1].strip())
                        exec_time, total_time = float(line[2].strip()), float(line[3].strip())

                        self.true_kruskal_ensemble_data[query] = [psql_opt_time, exec_time, total_time]

                with open(true_goo_data_file, "r") as input_f:
                    for idx, line in enumerate(input_f):
                        if idx == 0: continue
                        line = line.strip().split(",")

                        query, psql_opt_time = line[0].strip(), float(line[1].strip())
                        exec_time, total_time = float(line[2].strip()), float(line[3].strip())

                        self.true_goo_data[query] = [psql_opt_time, exec_time, total_time]

                with open(true_ensemble_data_file, "r") as input_f:
                    for idx, line in enumerate(input_f):
                        if idx == 0: continue
                        line = line.strip().split(",")

                        query, psql_opt_time = line[0].strip(), float(line[1].strip())
                        exec_time, total_time = float(line[2].strip()), float(line[3].strip())

                        self.true_ensemble_data[query] = [psql_opt_time, exec_time, total_time]

                with open(true_exhaustive_data_file, "r") as input_f:
                    for idx, line in enumerate(input_f):
                        if idx == 0: continue
                        line = line.strip().split(",")

                        query, psql_opt_time = line[0].strip(), float(line[1].strip())
                        exec_time, total_time = float(line[2].strip()), float(line[3].strip())

                        self.true_exhaustive_data[query] = [psql_opt_time, exec_time, total_time]

            def load_estimated_data(self):
                with open(est_prim_data_file, "r") as input_f:
                    for idx, line in enumerate(input_f):
                        if idx == 0: continue
                        line = line.strip().split(",")

                        query, psql_opt_time = line[0].strip(), float(line[1].strip())
                        exec_time, total_time = float(line[2].strip()), float(line[3].strip())

                        self.est_prim_data[query] = [psql_opt_time, exec_time, total_time]

                with open(est_prim_ensemble_data_file, "r") as input_f:
                    for idx, line in enumerate(input_f):
                        if idx == 0: continue
                        line = line.strip().split(",")

                        query, psql_opt_time = line[0].strip(), float(line[1].strip())
                        exec_time, total_time = float(line[2].strip()), float(line[3].strip())

                        self.est_prim_ensemble_data[query] = [psql_opt_time, exec_time, total_time]

                with open(est_kruskal_data_file, "r") as input_f:
                    for idx, line in enumerate(input_f):
                        if idx == 0: continue
                        line = line.strip().split(",")

                        query, psql_opt_time = line[0].strip(), float(line[1].strip())
                        exec_time, total_time = float(line[2].strip()), float(line[3].strip())

                        self.est_kruskal_data[query] = [psql_opt_time, exec_time, total_time]

                with open(est_kruskal_ensemble_data_file, "r") as input_f:
                    for idx, line in enumerate(input_f):
                        if idx == 0: continue
                        line = line.strip().split(",")

                        query, psql_opt_time = line[0].strip(), float(line[1].strip())
                        exec_time, total_time = float(line[2].strip()), float(line[3].strip())

                        self.est_kruskal_ensemble_data[query] = [psql_opt_time, exec_time, total_time]

                with open(est_goo_data_file, "r") as input_f:
                    for idx, line in enumerate(input_f):
                        if idx == 0: continue
                        line = line.strip().split(",")

                        query, psql_opt_time = line[0].strip(), float(line[1].strip())
                        exec_time, total_time = float(line[2].strip()), float(line[3].strip())

                        self.est_goo_data[query] = [psql_opt_time, exec_time, total_time]

                with open(est_ensemble_data_file, "r") as input_f:
                    for idx, line in enumerate(input_f):
                        if idx == 0: continue
                        line = line.strip().split(",")

                        query, psql_opt_time = line[0].strip(), float(line[1].strip())
                        exec_time, total_time = float(line[2].strip()), float(line[3].strip())

                        self.est_ensemble_data[query] = [psql_opt_time, exec_time, total_time]
                
                with open(est_exhaustive_data_file, "r") as input_f:
                    for idx, line in enumerate(input_f):
                        if idx == 0: continue
                        line = line.strip().split(",")

                        query, psql_opt_time = line[0].strip(), float(line[1].strip())
                        exec_time, total_time = float(line[2].strip()), float(line[3].strip())

                        self.est_exhaustive_data[query] = [psql_opt_time, exec_time, total_time]
                
            def generate_figure_true_data(self):

                header = ["query name", "Exhaustive", 
                            "Kruskal", "Kruskal Ensemble", 
                            "Prim", "Prim Ensemble", 
                            "Ensemble MST", "GOO"]
                
                output_f_exec = open(figure_9a_file, "w")
                output_f_exec_writer = csv.writer(output_f_exec, delimiter=',')
                output_f_exec_writer.writerow(header)

                output_f_opt = open(figure_10a_file, "w")
                output_f_opt_writer = csv.writer(output_f_opt, delimiter=',')
                output_f_opt_writer.writerow(header)

                output_f_table_exec = open(tables_1_exec_file, "w")
                output_f_table_exec_writer = csv.writer(output_f_table_exec, delimiter=',')
                output_f_table_exec_writer.writerow(["query complexity", "exhaustive",
                                                "kruskal", "prim", "ensemble", "goo"])
                
                output_f_table_opt = open(tables_1_psql_opt_file, "w")
                output_f_table_opt_writer = csv.writer(output_f_table_opt, delimiter=',')
                output_f_table_opt_writer.writerow(["query complexity", "exhaustive",
                                                "kruskal", "prim", "ensemble", "goo"])
                
                total_exhaustive_exec, total_kruskal_exec, total_prim_exec, total_ensemble_exec, total_goo_exec = 0, 0, 0, 0, 0
                total_exhaustive_opt, total_kruskal_opt, total_prim_opt, total_ensemble_opt, total_goo_opt = 0, 0, 0, 0, 0

                complexity_names = ["Simple", "Moderate", "Complex"]
                for idx, query_complexity in enumerate([self.simple_queries, self.moderate_queries, self.complex_queries]):

                    curr_exhaustive_exec, curr_kruskal_exec, curr_prim_exec, curr_ensemble_exec, curr_goo_exec = 0, 0, 0, 0, 0
                    curr_exhaustive_opt, curr_kruskal_opt, curr_prim_opt, curr_ensemble_opt, curr_goo_opt = 0, 0, 0, 0, 0

                    for query_family in sorted(query_complexity):
                        for query in sorted(query_complexity[query_family]):

                            if query in self.true_exhaustive_data:
                                exhaustive_mst_exec = self.true_exhaustive_data[query][1] / sec_time
                                exhaustive_mst_opt = self.true_exhaustive_data[query][0]
                            else: 
                                print(["True data exhaustive missing", query])
                                continue
                                exhaustive_mst_exec = 10**5
                                exhaustive_mst_opt = 10**5

                            curr_exhaustive_exec += exhaustive_mst_exec
                            curr_kruskal_exec += self.true_kruskal_data[query][1] / sec_time
                            curr_prim_exec += self.true_prim_data[query][1] / sec_time
                            curr_ensemble_exec += self.true_ensemble_data[query][1] / sec_time
                            curr_goo_exec += self.true_goo_data[query][1] / sec_time

                            curr_exhaustive_opt += exhaustive_mst_opt
                            curr_kruskal_opt += self.true_kruskal_data[query][0]
                            curr_prim_opt += self.true_prim_data[query][0]
                            curr_ensemble_opt += self.true_ensemble_data[query][0]
                            curr_goo_opt += self.true_goo_data[query][0]

                            output_f_exec_writer.writerow([query, 
                                exhaustive_mst_exec,
                                self.true_kruskal_data[query][1] / sec_time, 
                                self.true_kruskal_ensemble_data[query][1] / sec_time, 
                                self.true_prim_data[query][1] / sec_time, 
                                self.true_prim_ensemble_data[query][1] / sec_time, 
                                self.true_ensemble_data[query][1] / sec_time,
                                self.true_goo_data[query][1] / sec_time])
                            output_f_opt_writer.writerow([query, 
                                exhaustive_mst_opt,
                                self.true_kruskal_data[query][0], 
                                self.true_kruskal_ensemble_data[query][0], 
                                self.true_prim_data[query][0], 
                                self.true_prim_ensemble_data[query][0], 
                                self.true_ensemble_data[query][0],
                                self.true_goo_data[query][0]])
                        
                    total_exhaustive_exec += curr_exhaustive_exec
                    total_kruskal_exec += curr_kruskal_exec
                    total_prim_exec += curr_prim_exec
                    total_ensemble_exec += curr_ensemble_exec
                    total_goo_exec += curr_goo_exec

                    total_exhaustive_opt += curr_exhaustive_opt
                    total_kruskal_opt += curr_kruskal_opt
                    total_prim_opt += curr_prim_opt
                    total_ensemble_opt += curr_ensemble_opt
                    total_goo_opt += curr_goo_opt
                        
                    output_f_table_exec_writer.writerow([complexity_names[idx], curr_exhaustive_exec, curr_kruskal_exec, 
                                                    curr_prim_exec, curr_ensemble_exec, curr_goo_exec])
                    output_f_table_opt_writer.writerow([complexity_names[idx], curr_exhaustive_opt, curr_kruskal_opt, 
                                                    curr_prim_opt, curr_ensemble_opt, curr_goo_opt])
                
                output_f_table_exec_writer.writerow(["TOTAL", total_exhaustive_exec, total_kruskal_exec, 
                                                    total_prim_exec, total_ensemble_exec, total_goo_exec])
                output_f_table_opt_writer.writerow(["TOTAL", total_exhaustive_opt, total_kruskal_opt, 
                                                    total_prim_opt, total_ensemble_opt, total_goo_opt])

                output_f_exec.close()
                output_f_table_exec.close()
                output_f_table_opt.close()
                output_f_opt.close()

            def generate_figure_estimated_data(self):

                header = ["query name", "Exhaustive", 
                            "Kruskal", "Kruskal Ensemble", 
                            "Prim", "Prim Ensemble", 
                            "Ensemble MST", "GOO"]
                
                output_f_exec = open(figure_9b_file, "w")
                output_f_exec_writer = csv.writer(output_f_exec, delimiter=',')
                output_f_exec_writer.writerow(header)

                output_f_opt = open(figure_10b_file, "w")
                output_f_opt_writer = csv.writer(output_f_opt, delimiter=',')
                output_f_opt_writer.writerow(header)

                output_f_table_exec = open(tables_2_exec_file, "w")
                output_f_table_exec_writer = csv.writer(output_f_table_exec, delimiter=',')
                output_f_table_exec_writer.writerow(["query complexity", "exhaustive",
                                                "kruskal", "prim", "ensemble", "goo"])
                
                output_f_table_opt = open(tables_2_psql_opt_file, "w")
                output_f_table_opt_writer = csv.writer(output_f_table_opt, delimiter=',')
                output_f_table_opt_writer.writerow(["query complexity", "exhaustive",
                                                "kruskal", "prim", "ensemble", "goo"])
                
                total_exhaustive_exec, total_kruskal_exec, total_prim_exec, total_ensemble_exec, total_goo_exec = 0, 0, 0, 0, 0
                total_exhaustive_opt, total_kruskal_opt, total_prim_opt, total_ensemble_opt, total_goo_opt = 0, 0, 0, 0, 0

                complexity_names = ["Simple", "Moderate", "Complex"]
                for idx, query_complexity in enumerate([self.simple_queries, self.moderate_queries, self.complex_queries]):

                    curr_exhaustive_exec, curr_kruskal_exec, curr_prim_exec, curr_ensemble_exec, curr_goo_exec = 0, 0, 0, 0, 0
                    curr_exhaustive_opt, curr_kruskal_opt, curr_prim_opt, curr_ensemble_opt, curr_goo_opt = 0, 0, 0, 0, 0

                    for query_family in sorted(query_complexity):
                        for query in sorted(query_complexity[query_family]):

                            if query in self.est_exhaustive_data:
                                exhaustive_mst_exec = self.est_exhaustive_data[query][1] / sec_time
                                exhaustive_mst_opt = self.est_exhaustive_data[query][0]
                            else: 
                                print(["Est data exhaustive missing", query])
                                continue
                                exhaustive_mst_exec = 10**5
                                exhaustive_mst_opt = 10**5

                            curr_exhaustive_exec += exhaustive_mst_exec
                            curr_kruskal_exec += self.est_kruskal_data[query][1] / sec_time
                            curr_prim_exec += self.est_prim_data[query][1] / sec_time
                            curr_ensemble_exec += self.est_ensemble_data[query][1] / sec_time
                            curr_goo_exec += self.est_goo_data[query][1] / sec_time

                            curr_exhaustive_opt += exhaustive_mst_opt
                            curr_kruskal_opt += self.est_kruskal_data[query][0]
                            curr_prim_opt += self.est_prim_data[query][0]
                            curr_ensemble_opt += self.est_ensemble_data[query][0]
                            curr_goo_opt += self.est_goo_data[query][0]

                            output_f_exec_writer.writerow([query, 
                                exhaustive_mst_exec,
                                self.est_kruskal_data[query][1] / sec_time, 
                                self.est_kruskal_ensemble_data[query][1] / sec_time, 
                                self.est_prim_data[query][1] / sec_time, 
                                self.est_prim_ensemble_data[query][1] / sec_time, 
                                self.est_ensemble_data[query][1] / sec_time,
                                self.est_goo_data[query][1] / sec_time])
                            output_f_opt_writer.writerow([query, 
                                exhaustive_mst_opt,
                                self.est_kruskal_data[query][0], 
                                self.est_kruskal_ensemble_data[query][0], 
                                self.est_prim_data[query][0], 
                                self.est_prim_ensemble_data[query][0], 
                                self.est_ensemble_data[query][0],
                                self.est_goo_data[query][0]])
                        
                    total_exhaustive_exec += curr_exhaustive_exec
                    total_kruskal_exec += curr_kruskal_exec
                    total_prim_exec += curr_prim_exec
                    total_ensemble_exec += curr_ensemble_exec
                    total_goo_exec += curr_goo_exec

                    total_exhaustive_opt += curr_exhaustive_opt
                    total_kruskal_opt += curr_kruskal_opt
                    total_prim_opt += curr_prim_opt
                    total_ensemble_opt += curr_ensemble_opt
                    total_goo_opt += curr_goo_opt
                        
                    output_f_table_exec_writer.writerow([complexity_names[idx], curr_exhaustive_exec, curr_kruskal_exec, 
                                                    curr_prim_exec, curr_ensemble_exec, curr_goo_exec])
                    output_f_table_opt_writer.writerow([complexity_names[idx], curr_exhaustive_opt, curr_kruskal_opt, 
                                                    curr_prim_opt, curr_ensemble_opt, curr_goo_opt])
                
                output_f_table_exec_writer.writerow(["TOTAL", total_exhaustive_exec, total_kruskal_exec, 
                                                    total_prim_exec, total_ensemble_exec, total_goo_exec])
                output_f_table_opt_writer.writerow(["TOTAL", total_exhaustive_opt, total_kruskal_opt, 
                                                    total_prim_opt, total_ensemble_opt, total_goo_opt])

                output_f_exec.close()
                output_f_table_exec.close()
                output_f_table_opt.close()
                output_f_opt.close()
                
        GenerateFigures()

        ####################################
        
        print("\nSuccess.\n")
    except:
        print("Script errors.\n")
