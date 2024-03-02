import os, csv, sys

print(
"\n \
1. Enter: ~/mst_query_optimization\n \
2. Run the following command: /usr/bin/python3 scripts_cost/mst_top_cout_generate_ensemble.py\n \
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

            out_folder = "output_data/topology/costs_cout/"
            enumeration_ensemble = ["kruskal_ensemble", "prim_ensemble"]

            output_cost_csv_file = "output_data/topology/costs_cout/ensemble_opt_plans.csv"
            output_cost_f = open(output_cost_csv_file, "w")
            output_cost_f_writer = csv.writer(output_cost_f, delimiter=',')
            output_cost_f_writer.writerow(["query_name", "optimization_time_(ms)", "est_cost", "true_cost", "plan", "physical_plan"])

            ############################ Query Cost and Runtime #########################################################

            all_cost_data = [{}, {}]
            for mst_idx, mst_type in enumerate(enumeration_ensemble):

                query_cost_plans_file = out_folder + mst_type + "_opt_plans.csv"

                with open(query_cost_plans_file, "r") as input_f:
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

            ##################################### Build Ensemble ########################################################

            for f_idx, file_name in enumerate(sorted(os.listdir(input_queries))):
                query = file_name[:-4]

                if query not in all_cost_data[0] or query not in all_cost_data[1]:
                    print(["missing enum cost", cardinality_idx, query])
                    continue

                kruskal_cost, prim_cost = all_cost_data[0][query], all_cost_data[1][query]
                kruskal_cost[1] += prim_cost[1]  # NOTE: optimization time is Kruskal + Prim
                prim_cost[1] += kruskal_cost[1]  # NOTE: optimization time is Kruskal + Prim

                card_type = 3

                if prim_cost[card_type] < kruskal_cost[card_type]: 
                    output_cost_f_writer.writerow(prim_cost)
                else: 
                    output_cost_f_writer.writerow(kruskal_cost)

            output_cost_f.close()

            ####################################

            print("Success.\n")
    except:
        print("Wrong parameter type or code error.\n")
