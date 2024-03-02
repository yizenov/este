import os, csv, sys

print(
"\n \
1. Enter: ~/mst_query_optimization\n \
2. Run the following command: /usr/bin/python3 figures/generate_coverage_figures_data.py\n \
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
        true_prim_plan_data_file = "output_data/job/plans_subplans/prim_ensemble_all_plans.csv"
        true_kruskal_plan_data_file = "output_data/job/plans_subplans/kruskal_ensemble_all_plans.csv"
        true_prim_subplan_data_file = "output_data/job/plans_subplans/prim_ensemble_sub_plans.csv"
        true_kruskal_subplan_data_file = "output_data/job/plans_subplans/kruskal_ensemble_sub_plans.csv"

        # estimated data
        est_prim_plan_data_file = "output_data/job/plans_subplans/prim_ensemble_all_plans_psql.csv"
        est_kruskal_plan_data_file = "output_data/job/plans_subplans/kruskal_ensemble_all_plans_psql.csv"
        est_prim_subplan_data_file = "output_data/job/plans_subplans/prim_ensemble_sub_plans_psql.csv"
        est_kruskal_subplan_data_file = "output_data/job/plans_subplans/kruskal_ensemble_sub_plans_psql.csv"

        figure_sub_true_file = "figures/figures_data/figure_coverage_subplan_data_true.csv"
        figure_plan_true_file = "figures/figures_data/figure_coverage_plan_data_true.csv"

        figure_sub_est_file = "figures/figures_data/figure_coverage_subplan_data_est.csv"
        figure_plan_est_file = "figures/figures_data/figure_coverage_plan_data_est.csv"

        class GenerateFigures(object):
            def __init__(self):

                self.simple_queries = {}
                self.moderate_queries = {}
                self.complex_queries = {}

                self.true_prim_plan_data, self.est_prim_plan_data = {}, {}
                self.true_kruskal_plan_data, self.est_kruskal_plan_data = {}, {}

                self.true_prim_subplan_data, self.est_prim_subplan_data = {}, {}
                self.true_kruskal_subplan_data, self.est_kruskal_subplan_data = {}, {}

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
                with open(true_prim_plan_data_file, "r") as input_f:
                    for idx, line in enumerate(input_f):
                        if idx == 0: continue
                        line = line.strip().split(",")

                        query = line[0].strip()
                        est_cost, true_cost = float(line[1].strip()), float(line[2].strip())
                        edges, physical_plan = line[3].strip(), line[4].strip()

                        if query not in self.true_prim_plan_data: self.true_prim_plan_data[query] = []
                        self.true_prim_plan_data[query].append([est_cost, true_cost, edges, physical_plan])

                with open(true_prim_subplan_data_file, "r") as input_f:
                    for idx, line in enumerate(input_f):
                        if idx == 0: continue
                        line = line.strip().split(",")

                        query = line[0].strip()
                        covered_plan, all_plans = line[1].strip(), float(line[2].strip())

                        if query not in self.true_prim_subplan_data: self.true_prim_subplan_data[query] = []
                        self.true_prim_subplan_data[query].append([covered_plan, all_plans])

                with open(true_kruskal_plan_data_file, "r") as input_f:
                    for idx, line in enumerate(input_f):
                        if idx == 0: continue
                        line = line.strip().split(",")

                        query = line[0].strip()
                        est_cost, true_cost = float(line[1].strip()), float(line[2].strip())
                        edges, physical_plan = line[3].strip(), line[4].strip()

                        if query not in self.true_kruskal_plan_data: self.true_kruskal_plan_data[query] = []
                        self.true_kruskal_plan_data[query].append([est_cost, true_cost, edges, physical_plan])

                with open(true_kruskal_subplan_data_file, "r") as input_f:
                    for idx, line in enumerate(input_f):
                        if idx == 0: continue
                        line = line.strip().split(",")

                        query = line[0].strip()
                        covered_plan, all_plans = line[1].strip(), float(line[2].strip())

                        if query not in self.true_kruskal_subplan_data: self.true_kruskal_subplan_data[query] = []
                        self.true_kruskal_subplan_data[query].append([covered_plan, all_plans])

            def load_estimated_data(self):
                with open(est_prim_plan_data_file, "r") as input_f:
                    for idx, line in enumerate(input_f):
                        if idx == 0: continue
                        line = line.strip().split(",")

                        query = line[0].strip()
                        est_cost, true_cost = float(line[1].strip()), float(line[2].strip())
                        edges, physical_plan = line[3].strip(), line[4].strip()

                        if query not in self.est_prim_plan_data: self.est_prim_plan_data[query] = []
                        self.est_prim_plan_data[query].append([est_cost, true_cost, edges, physical_plan])

                with open(est_prim_subplan_data_file, "r") as input_f:
                    for idx, line in enumerate(input_f):
                        if idx == 0: continue
                        line = line.strip().split(",")

                        query = line[0].strip()
                        covered_plan, all_plans = line[1].strip(), float(line[2].strip())

                        if query not in self.est_prim_subplan_data: self.est_prim_subplan_data[query] = []
                        self.est_prim_subplan_data[query].append([covered_plan, all_plans])

                with open(est_kruskal_plan_data_file, "r") as input_f:
                    for idx, line in enumerate(input_f):
                        if idx == 0: continue
                        line = line.strip().split(",")

                        query = line[0].strip()
                        est_cost, true_cost = float(line[1].strip()), float(line[2].strip())
                        edges, physical_plan = line[3].strip(), line[4].strip()

                        if query not in self.est_kruskal_plan_data: self.est_kruskal_plan_data[query] = []
                        self.est_kruskal_plan_data[query].append([est_cost, true_cost, edges, physical_plan])

                with open(est_kruskal_subplan_data_file, "r") as input_f:
                    for idx, line in enumerate(input_f):
                        if idx == 0: continue
                        line = line.strip().split(",")

                        query = line[0].strip()
                        covered_plan, all_plans = line[1].strip(), float(line[2].strip())

                        if query not in self.est_kruskal_subplan_data: self.est_kruskal_subplan_data[query] = []
                        self.est_kruskal_subplan_data[query].append([covered_plan, all_plans])
                
            def generate_figure_true_data(self):
                
                output_f_sub = open(figure_sub_true_file, "w")
                output_f_sub_writer = csv.writer(output_f_sub, delimiter=',')
                output_f_sub_writer.writerow(["query_name", "total_subplans", "prim_subplans", "kruskal_subplans", "distinct_subplans", "duplicate_subplans"])

                output_f_plan = open(figure_plan_true_file, "w")
                output_f_plan_writer = csv.writer(output_f_plan, delimiter=',')
                output_f_plan_writer.writerow(["query_name", "prim_plans", "kruskal_plans", "distinct_plans", "duplicate_plans"])
                
                complexity_names = ["Simple", "Moderate", "Complex"]
                for idx, query_complexity in enumerate([self.simple_queries, self.moderate_queries, self.complex_queries]):
                    for query_family in sorted(query_complexity):
                        for query in sorted(query_complexity[query_family]):
                            distinct_plans, distinct_subplans = set(), set()
                            rep_distinct_plans, rep_distinct_subplans = set(), set()
                            total_plans, total_subplans = -1, -1
                            
                            for plan_ in self.true_prim_plan_data[query]:
                                distinct_plans.add(plan_[3])
                            for plan_ in self.true_kruskal_plan_data[query]:
                                if plan_[3] in distinct_plans:
                                    rep_distinct_plans.add(plan_[3])
                                distinct_plans.add(plan_[3])

                            for subplan_ in self.true_prim_subplan_data[query]:
                                total_subplans = subplan_[1]
                                distinct_subplans.add(subplan_[0])
                            for subplan_ in self.true_kruskal_subplan_data[query]:
                                if subplan_[0] in distinct_subplans:
                                    rep_distinct_subplans.add(subplan_[0])
                                distinct_subplans.add(subplan_[0])

                            output_f_plan_writer.writerow([query, query_complexity[query_family][query], 
                                                len(self.true_prim_plan_data[query]), 
                                                len(self.true_kruskal_plan_data[query]), len(distinct_plans),
                                                len(rep_distinct_plans)])
                            output_f_sub_writer.writerow([query, query_complexity[query_family][query],
                                                total_subplans, len(self.true_prim_subplan_data[query]),
                                                len(self.true_kruskal_subplan_data[query]), len(distinct_subplans),
                                                len(rep_distinct_subplans)])
                output_f_sub.close()
                output_f_plan.close()

            def generate_figure_estimated_data(self):

                output_f_sub = open(figure_sub_est_file, "w")
                output_f_sub_writer = csv.writer(output_f_sub, delimiter=',')
                output_f_sub_writer.writerow(["query_name", "query_complexity", "total_subplans", "prim_subplans", "kruskal_subplans", "distinct_subplans", "duplicate_subplans"])

                output_f_plan = open(figure_plan_est_file, "w")
                output_f_plan_writer = csv.writer(output_f_plan, delimiter=',')
                output_f_plan_writer.writerow(["query_name", "query_complexity", "prim_plans", "kruskal_plans", "distinct_plans", "duplicate_plans"])
                
                complexity_names = ["Simple", "Moderate", "Complex"]
                for idx, query_complexity in enumerate([self.simple_queries, self.moderate_queries, self.complex_queries]):
                    for query_family in sorted(query_complexity):
                        for query in sorted(query_complexity[query_family]):
                            distinct_plans, distinct_subplans = set(), set()
                            rep_distinct_plans, rep_distinct_subplans = set(), set()
                            total_plans, total_subplans = -1, -1
                            
                            for plan_ in self.est_prim_plan_data[query]:
                                distinct_plans.add(plan_[3])
                            for plan_ in self.est_kruskal_plan_data[query]:
                                if plan_[3] in distinct_plans:
                                    rep_distinct_plans.add(plan_[3])
                                distinct_plans.add(plan_[3])

                            for subplan_ in self.est_prim_subplan_data[query]:
                                total_subplans = subplan_[1]
                                distinct_subplans.add(subplan_[0])
                            for subplan_ in self.est_kruskal_subplan_data[query]:
                                if subplan_[0] in distinct_subplans:
                                    rep_distinct_subplans.add(subplan_[0])
                                distinct_subplans.add(subplan_[0])

                            output_f_plan_writer.writerow([query, len(self.est_prim_plan_data[query]), 
                                                len(self.est_kruskal_plan_data[query]), len(distinct_plans),
                                                len(rep_distinct_plans)])
                            output_f_sub_writer.writerow([query, total_subplans, len(self.est_prim_subplan_data[query]),
                                                len(self.est_kruskal_subplan_data[query]), len(distinct_subplans),
                                                len(rep_distinct_subplans)])
                output_f_sub.close()
                output_f_plan.close()
                
        GenerateFigures()

        ####################################
        
        print("\nSuccess.\n")
    except:
        print("Script errors.\n")
