import os, csv, sys

print(
"\n \
1. Enter: ~/mst_query_optimization\n \
2. Run the following command: /usr/bin/python3 figures/generate_cost_figure_data.py\n \
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

        # true data
        true_prim_data_file = "output_data/job/costs/prim_opt_plans.csv"
        true_prim_ensemble_data_file = "output_data/job/costs/prim_ensemble_opt_plans.csv"
        true_kruskal_data_file = "output_data/job/costs/kruskal_opt_plans.csv"
        true_kruskal_ensemble_data_file = "output_data/job/costs/kruskal_ensemble_opt_plans.csv"
        true_goo_data_file = "output_data/job/costs/goo_opt_plans.csv"
        true_ensemble_data_file = "output_data/job/costs/ensemble_opt_plans.csv"
        true_exhaustive_data_file = "output_data/job/costs/exhaustive_opt_plans.csv"

        # estimated data
        est_prim_data_file = "output_data/job/costs/prim_opt_plans_psql.csv"
        est_prim_ensemble_data_file = "output_data/job/costs/prim_ensemble_opt_plans_psql.csv"
        est_kruskal_data_file = "output_data/job/costs/kruskal_opt_plans_psql.csv"
        est_kruskal_ensemble_data_file = "output_data/job/costs/kruskal_ensemble_opt_plans_psql.csv"
        est_goo_data_file = "output_data/job/costs/goo_opt_plans_psql.csv"
        est_ensemble_data_file = "output_data/job/costs/ensemble_opt_plans_psql.csv"
        est_exhaustive_data_file = "output_data/job/costs/exhaustive_opt_plans_psql.csv"

        figure_7a_file = "figures/figures_data/figure_7a_data.csv"  # true cost
        tables_1_cost_file = "figures/figures_data/tables_1_cost_data.csv"  # true cost table
        figure_8a_file = "figures/figures_data/figure_8a_data.csv" # true opt-time
        tables_1_opt_file = "figures/figures_data/tables_1_opt_data.csv"  # true opt-time table

        figure_7b_file = "figures/figures_data/figure_7b_data.csv"  # est cost
        tables_2_cost_file = "figures/figures_data/tables_2_cost_data.csv"  # est cost table
        figure_8b_file = "figures/figures_data/figure_8b_data.csv"  # est opt-time
        tables_2_opt_file = "figures/figures_data/tables_2_opt_data.csv"  # est opt-time table

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

                        query, opt_time = line[0].strip(), float(line[1].strip())
                        est_cost, true_cost = float(line[2].strip()), float(line[3].strip())
                        edges = line[4].strip().split()

                        self.true_prim_data[query] = [opt_time, est_cost, true_cost, edges]

                with open(true_prim_ensemble_data_file, "r") as input_f:
                    for idx, line in enumerate(input_f):
                        if idx == 0: continue
                        line = line.strip().split(",")

                        query, opt_time = line[0].strip(), float(line[1].strip())
                        est_cost, true_cost = float(line[2].strip()), float(line[3].strip())
                        edges = line[4].strip().split()

                        self.true_prim_ensemble_data[query] = [opt_time, est_cost, true_cost, edges]

                with open(true_kruskal_data_file, "r") as input_f:
                    for idx, line in enumerate(input_f):
                        if idx == 0: continue
                        line = line.strip().split(",")

                        query, opt_time = line[0].strip(), float(line[1].strip())
                        est_cost, true_cost = float(line[2].strip()), float(line[3].strip())
                        edges = line[4].strip().split()

                        self.true_kruskal_data[query] = [opt_time, est_cost, true_cost, edges]

                with open(true_kruskal_ensemble_data_file, "r") as input_f:
                    for idx, line in enumerate(input_f):
                        if idx == 0: continue
                        line = line.strip().split(",")

                        query, opt_time = line[0].strip(), float(line[1].strip())
                        est_cost, true_cost = float(line[2].strip()), float(line[3].strip())
                        edges = line[4].strip().split()

                        self.true_kruskal_ensemble_data[query] = [opt_time, est_cost, true_cost, edges]

                with open(true_goo_data_file, "r") as input_f:
                    for idx, line in enumerate(input_f):
                        if idx == 0: continue
                        line = line.strip().split(",")

                        query, opt_time = line[0].strip(), float(line[1].strip())
                        est_cost, true_cost = float(line[2].strip()), float(line[3].strip())
                        edges = line[4].strip().split()

                        self.true_goo_data[query] = [opt_time, est_cost, true_cost, edges]

                with open(true_ensemble_data_file, "r") as input_f:
                    for idx, line in enumerate(input_f):
                        if idx == 0: continue
                        line = line.strip().split(",")

                        query, opt_time = line[0].strip(), float(line[1].strip())
                        est_cost, true_cost = float(line[2].strip()), float(line[3].strip())
                        edges = line[4].strip().split()

                        self.true_ensemble_data[query] = [opt_time, est_cost, true_cost, edges]

                with open(true_exhaustive_data_file, "r") as input_f:
                    for idx, line in enumerate(input_f):
                        if idx == 0: continue
                        line = line.strip().split(",")

                        query, opt_time = line[0].strip(), float(line[1].strip())
                        est_cost, true_cost = float(line[2].strip()), float(line[3].strip())
                        edges = line[4].strip().split()

                        self.true_exhaustive_data[query] = [opt_time, est_cost, true_cost, edges]

            def load_estimated_data(self):
                with open(est_prim_data_file, "r") as input_f:
                    for idx, line in enumerate(input_f):
                        if idx == 0: continue
                        line = line.strip().split(",")

                        query, opt_time = line[0].strip(), float(line[1].strip())
                        est_cost, true_cost = float(line[2].strip()), float(line[3].strip())
                        edges = line[4].strip().split()

                        self.est_prim_data[query] = [opt_time, est_cost, true_cost, edges]

                with open(est_prim_ensemble_data_file, "r") as input_f:
                    for idx, line in enumerate(input_f):
                        if idx == 0: continue
                        line = line.strip().split(",")

                        query, opt_time = line[0].strip(), float(line[1].strip())
                        est_cost, true_cost = float(line[2].strip()), float(line[3].strip())
                        edges = line[4].strip().split()

                        self.est_prim_ensemble_data[query] = [opt_time, est_cost, true_cost, edges]

                with open(est_kruskal_data_file, "r") as input_f:
                    for idx, line in enumerate(input_f):
                        if idx == 0: continue
                        line = line.strip().split(",")

                        query, opt_time = line[0].strip(), float(line[1].strip())
                        est_cost, true_cost = float(line[2].strip()), float(line[3].strip())
                        edges = line[4].strip().split()

                        self.est_kruskal_data[query] = [opt_time, est_cost, true_cost, edges]

                with open(est_kruskal_ensemble_data_file, "r") as input_f:
                    for idx, line in enumerate(input_f):
                        if idx == 0: continue
                        line = line.strip().split(",")

                        query, opt_time = line[0].strip(), float(line[1].strip())
                        est_cost, true_cost = float(line[2].strip()), float(line[3].strip())
                        edges = line[4].strip().split()

                        self.est_kruskal_ensemble_data[query] = [opt_time, est_cost, true_cost, edges]

                with open(est_goo_data_file, "r") as input_f:
                    for idx, line in enumerate(input_f):
                        if idx == 0: continue
                        line = line.strip().split(",")

                        query, opt_time = line[0].strip(), float(line[1].strip())
                        est_cost, true_cost = float(line[2].strip()), float(line[3].strip())
                        edges = line[4].strip().split()

                        self.est_goo_data[query] = [opt_time, est_cost, true_cost, edges]

                with open(est_ensemble_data_file, "r") as input_f:
                    for idx, line in enumerate(input_f):
                        if idx == 0: continue
                        line = line.strip().split(",")

                        query, opt_time = line[0].strip(), float(line[1].strip())
                        est_cost, true_cost = float(line[2].strip()), float(line[3].strip())
                        edges = line[4].strip().split()

                        self.est_ensemble_data[query] = [opt_time, est_cost, true_cost, edges]
                
                with open(est_exhaustive_data_file, "r") as input_f:
                    for idx, line in enumerate(input_f):
                        if idx == 0: continue
                        line = line.strip().split(",")

                        query, opt_time = line[0].strip(), float(line[1].strip())
                        est_cost, true_cost = float(line[2].strip()), float(line[3].strip())
                        edges = line[4].strip().split()

                        self.est_exhaustive_data[query] = [opt_time, est_cost, true_cost, edges]
                
            def generate_figure_true_data(self):

                header = ["query name", "Exhaustive", 
                            "Kruskal", "Kruskal Ensemble", 
                            "Prim", "Prim Ensemble", 
                            "Ensemble MST", "GOO"]
                
                output_f_cost = open(figure_7a_file, "w")
                output_f_cost_writer = csv.writer(output_f_cost, delimiter=',')
                output_f_cost_writer.writerow(header)

                output_f_opt = open(figure_8a_file, "w")
                output_f_opt_writer = csv.writer(output_f_opt, delimiter=',')
                output_f_opt_writer.writerow(header)

                output_f_table_cost = open(tables_1_cost_file, "w")
                output_f_table_cost_writer = csv.writer(output_f_table_cost, delimiter=',')
                output_f_table_cost_writer.writerow(["query complexity", "exhaustive",
                                                "kruskal", "prim", "ensemble", "goo"])
                
                output_f_table_opt = open(tables_1_opt_file, "w")
                output_f_table_opt_writer = csv.writer(output_f_table_opt, delimiter=',')
                output_f_table_opt_writer.writerow(["query complexity", "exhaustive",
                                                "kruskal", "prim", "ensemble", "goo"])
                
                total_exhaustive_cost, total_kruskal_cost, total_prim_cost, total_ensemble_cost, total_goo_cost = 0, 0, 0, 0, 0
                total_exhaustive_opt, total_kruskal_opt, total_prim_opt, total_ensemble_opt, total_goo_opt = 0, 0, 0, 0, 0

                complexity_names = ["Simple", "Moderate", "Complex"]
                for idx, query_complexity in enumerate([self.simple_queries, self.moderate_queries, self.complex_queries]):

                    curr_exhaustive_cost, curr_kruskal_cost, curr_prim_cost, curr_ensemble_cost, curr_goo_cost = 0, 0, 0, 0, 0
                    curr_exhaustive_opt, curr_kruskal_opt, curr_prim_opt, curr_ensemble_opt, curr_goo_opt = 0, 0, 0, 0, 0

                    for query_family in sorted(query_complexity):
                        for query in sorted(query_complexity[query_family]):

                            if query in self.true_exhaustive_data:
                                exhaustive_mst_cost = self.true_exhaustive_data[query][2]
                                exhaustive_mst_opt = self.true_exhaustive_data[query][0]
                            else: 
                                print(["True data exhaustive missing", query])
                                continue
                                exhaustive_mst_cost = 10**5
                                exhaustive_mst_opt = 10**5

                            curr_exhaustive_cost += exhaustive_mst_cost
                            curr_kruskal_cost += self.true_kruskal_data[query][2]
                            curr_prim_cost += self.true_prim_data[query][2]
                            curr_ensemble_cost += self.true_ensemble_data[query][2]
                            curr_goo_cost += self.true_goo_data[query][2]

                            curr_exhaustive_opt += exhaustive_mst_opt
                            curr_kruskal_opt += self.true_kruskal_data[query][0]
                            curr_prim_opt += self.true_prim_data[query][0]
                            curr_ensemble_opt += self.true_ensemble_data[query][0]
                            curr_goo_opt += self.true_goo_data[query][0]

                            output_f_cost_writer.writerow([query, 
                                exhaustive_mst_cost,
                                self.true_kruskal_data[query][2], 
                                self.true_kruskal_ensemble_data[query][2], 
                                self.true_prim_data[query][2], 
                                self.true_prim_ensemble_data[query][2], 
                                self.true_ensemble_data[query][2],
                                self.true_goo_data[query][2]])
                            output_f_opt_writer.writerow([query, 
                                exhaustive_mst_opt,
                                self.true_kruskal_data[query][0], 
                                self.true_kruskal_ensemble_data[query][0], 
                                self.true_prim_data[query][0], 
                                self.true_prim_ensemble_data[query][0], 
                                self.true_ensemble_data[query][0],
                                self.true_goo_data[query][0]])
                        
                    total_exhaustive_cost += curr_exhaustive_cost
                    total_kruskal_cost += curr_kruskal_cost
                    total_prim_cost += curr_prim_cost
                    total_ensemble_cost += curr_ensemble_cost
                    total_goo_cost += curr_goo_cost

                    curr_kruskal_cost /= curr_exhaustive_cost
                    curr_prim_cost /= curr_exhaustive_cost
                    curr_ensemble_cost /= curr_exhaustive_cost
                    curr_goo_cost /= curr_exhaustive_cost
                    curr_exhaustive_cost /= curr_exhaustive_cost

                    total_exhaustive_opt += curr_exhaustive_opt
                    total_kruskal_opt += curr_kruskal_opt
                    total_prim_opt += curr_prim_opt
                    total_ensemble_opt += curr_ensemble_opt
                    total_goo_opt += curr_goo_opt
                        
                    output_f_table_cost_writer.writerow([complexity_names[idx], curr_exhaustive_cost, curr_kruskal_cost, 
                                                    curr_prim_cost, curr_ensemble_cost, curr_goo_cost])
                    output_f_table_opt_writer.writerow([complexity_names[idx], curr_exhaustive_opt, curr_kruskal_opt, 
                                                    curr_prim_opt, curr_ensemble_opt, curr_goo_opt])
                
                total_kruskal_cost /= total_exhaustive_cost
                total_prim_cost /= total_exhaustive_cost
                total_ensemble_cost /= total_exhaustive_cost
                total_goo_cost /= total_exhaustive_cost
                total_exhaustive_cost /= total_exhaustive_cost
                
                output_f_table_cost_writer.writerow(["TOTAL", total_exhaustive_cost, total_kruskal_cost, 
                                                    total_prim_cost, total_ensemble_cost, total_goo_cost])
                output_f_table_opt_writer.writerow(["TOTAL", total_exhaustive_opt, total_kruskal_opt, 
                                                    total_prim_opt, total_ensemble_opt, total_goo_opt])

                output_f_cost.close()
                output_f_table_cost.close()
                output_f_table_opt.close()
                output_f_opt.close()

            def generate_figure_estimated_data(self):

                header = ["query name", "Exhaustive", 
                            "Kruskal", "Kruskal Ensemble", 
                            "Prim", "Prim Ensemble", 
                            "Ensemble MST", "GOO"]
                
                output_f_cost = open(figure_7b_file, "w")
                output_f_cost_writer = csv.writer(output_f_cost, delimiter=',')
                output_f_cost_writer.writerow(header)

                output_f_opt = open(figure_8b_file, "w")
                output_f_opt_writer = csv.writer(output_f_opt, delimiter=',')
                output_f_opt_writer.writerow(header)

                output_f_table_cost = open(tables_2_cost_file, "w")
                output_f_table_cost_writer = csv.writer(output_f_table_cost, delimiter=',')
                output_f_table_cost_writer.writerow(["query complexity", "exhaustive",
                                                "kruskal", "prim", "ensemble", "goo"])
                
                output_f_table_opt = open(tables_2_opt_file, "w")
                output_f_table_opt_writer = csv.writer(output_f_table_opt, delimiter=',')
                output_f_table_opt_writer.writerow(["query complexity", "exhaustive",
                                                "kruskal", "prim", "ensemble", "goo"])
                
                total_exhaustive_cost, total_kruskal_cost, total_prim_cost, total_ensemble_cost, total_goo_cost = 0, 0, 0, 0, 0
                total_exhaustive_opt, total_kruskal_opt, total_prim_opt, total_ensemble_opt, total_goo_opt = 0, 0, 0, 0, 0

                complexity_names = ["Simple", "Moderate", "Complex"]
                for idx, query_complexity in enumerate([self.simple_queries, self.moderate_queries, self.complex_queries]):

                    curr_exhaustive_cost, curr_kruskal_cost, curr_prim_cost, curr_ensemble_cost, curr_goo_cost = 0, 0, 0, 0, 0
                    curr_exhaustive_opt, curr_kruskal_opt, curr_prim_opt, curr_ensemble_opt, curr_goo_opt = 0, 0, 0, 0, 0

                    for query_family in sorted(query_complexity):
                        for query in sorted(query_complexity[query_family]):

                            if query in self.est_exhaustive_data:
                                exhaustive_mst_cost = self.est_exhaustive_data[query][2]
                                exhaustive_mst_opt = self.est_exhaustive_data[query][0]
                            else: 
                                print(["Est data exhaustive missing", query])
                                continue
                                exhaustive_mst_cost = 10**5
                                exhaustive_mst_opt = 10**5

                            curr_exhaustive_cost += exhaustive_mst_cost
                            curr_kruskal_cost += self.est_kruskal_data[query][2]
                            curr_prim_cost += self.est_prim_data[query][2]
                            curr_ensemble_cost += self.est_ensemble_data[query][2]
                            curr_goo_cost += self.est_goo_data[query][2]

                            curr_exhaustive_opt += exhaustive_mst_opt
                            curr_kruskal_opt += self.est_kruskal_data[query][0]
                            curr_prim_opt += self.est_prim_data[query][0]
                            curr_ensemble_opt += self.est_ensemble_data[query][0]
                            curr_goo_opt += self.est_goo_data[query][0]

                            output_f_cost_writer.writerow([query, 
                                exhaustive_mst_cost,
                                self.est_kruskal_data[query][2], 
                                self.est_kruskal_ensemble_data[query][2], 
                                self.est_prim_data[query][2], 
                                self.est_prim_ensemble_data[query][2], 
                                self.est_ensemble_data[query][2],
                                self.est_goo_data[query][2]])
                            output_f_opt_writer.writerow([query, 
                                exhaustive_mst_opt,
                                self.est_kruskal_data[query][0], 
                                self.est_kruskal_ensemble_data[query][0], 
                                self.est_prim_data[query][0], 
                                self.est_prim_ensemble_data[query][0], 
                                self.est_ensemble_data[query][0],
                                self.est_goo_data[query][0]])
                        
                    total_exhaustive_cost += curr_exhaustive_cost
                    total_kruskal_cost += curr_kruskal_cost
                    total_prim_cost += curr_prim_cost
                    total_ensemble_cost += curr_ensemble_cost
                    total_goo_cost += curr_goo_cost

                    curr_kruskal_cost /= curr_exhaustive_cost
                    curr_prim_cost /= curr_exhaustive_cost
                    curr_ensemble_cost /= curr_exhaustive_cost
                    curr_goo_cost /= curr_exhaustive_cost
                    curr_exhaustive_cost /= curr_exhaustive_cost

                    total_exhaustive_opt += curr_exhaustive_opt
                    total_kruskal_opt += curr_kruskal_opt
                    total_prim_opt += curr_prim_opt
                    total_ensemble_opt += curr_ensemble_opt
                    total_goo_opt += curr_goo_opt
                        
                    output_f_table_cost_writer.writerow([complexity_names[idx], curr_exhaustive_cost, curr_kruskal_cost, 
                                                    curr_prim_cost, curr_ensemble_cost, curr_goo_cost])
                    output_f_table_opt_writer.writerow([complexity_names[idx], curr_exhaustive_opt, curr_kruskal_opt, 
                                                    curr_prim_opt, curr_ensemble_opt, curr_goo_opt])
                
                total_kruskal_cost /= total_exhaustive_cost
                total_prim_cost /= total_exhaustive_cost
                total_ensemble_cost /= total_exhaustive_cost
                total_goo_cost /= total_exhaustive_cost
                total_exhaustive_cost /= total_exhaustive_cost
                
                output_f_table_cost_writer.writerow(["TOTAL", total_exhaustive_cost, total_kruskal_cost, 
                                                    total_prim_cost, total_ensemble_cost, total_goo_cost])
                output_f_table_opt_writer.writerow(["TOTAL", total_exhaustive_opt, total_kruskal_opt, 
                                                    total_prim_opt, total_ensemble_opt, total_goo_opt])

                output_f_cost.close()
                output_f_table_cost.close()
                output_f_table_opt.close()
                output_f_opt.close()
                
        GenerateFigures()

        ####################################
        
        print("\nSuccess.\n")
    except:
        print("Script errors.\n")
