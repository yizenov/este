import os, sys

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

print(
"\n \
1. Enter: ~/mst_query_optimization\n \
2. Run the following command: /usr/bin/python3 figures/generate_cost_cout_boxplots.py\n \
\t Script requires 0 argument\n \
")

print('Number of arguments:', len(sys.argv) - 1)
print('Argument List:', str(sys.argv[1:]), '\n')

if len(sys.argv) != 1:
    print("Wrong number of arguments.\n")
else:
    try:
        root_begin = "~/mst_query_optimization/"
        input_queries = root_begin + "input_data/topology/workload_queries"
        cost_data_folder = root_begin + "output_data/topology/costs_cout"

        topology_names = {"0": "chain", "1": "cycle", "2": "star", "3": "clique"}

        figure_file_name = "figures/topology_cout_box_plots_"
        
        class GenerateFigures(object):
            def __init__(self):

                self.queries = {}
                self.cost_data = {}

                self.load_queries()
                self.load_cost_data()
                self.print_data()

            def load_queries(self):
                for file_name in sorted(os.listdir(input_queries)):
                    topology = file_name.split("_")[0]
                    relations = file_name.split("_")[1]
                    query_name = file_name.split("_")[2]

                    if topology not in self.queries: self.queries[topology] = {}
                    if relations not in self.queries[topology]: 
                        self.queries[topology][relations] = set()
                    self.queries[topology][relations].add(query_name)

            def load_cost_data(self):
                for file_name in sorted(os.listdir(cost_data_folder)):
                    enum_name = "_".join(file_name.split("_")[:-2])
                    self.cost_data[enum_name] = {}

                    with open(cost_data_folder + "/" + file_name, "r") as input_f:
                        for idx, line in enumerate(input_f):
                            if idx == 0: continue
                            line = line.strip().split(",")

                            topology = line[0].split(".")[0].split("_")[0]
                            relations = line[0].split(".")[0].split("_")[1]
                            query_name = line[0].split(".")[0].split("_")[2]
                            true_cost = float(line[3].strip())

                            if topology not in self.cost_data[enum_name]: 
                                self.cost_data[enum_name][topology] = {}
                            if relations not in self.cost_data[enum_name][topology]: 
                                self.cost_data[enum_name][topology][relations] = {}
                            if query_name not in self.cost_data[enum_name][topology][relations]: 
                                self.cost_data[enum_name][topology][relations][query_name] = true_cost
                            else: print("double query")
            
            def print_data(self):
                for t_idx, topology in enumerate(self.queries):
                    
                    all_dfs = []
                    target_enums_print = ["IK-KBZ", "LinearizedDP", "GOO", "A* (BU)", "A* (TD)", "ESTE"]
                    target_enums = ["ikkbz", "lindp", "goo", "hsearch_bu", "hsearch_td", "ensemble"]
                    for relations in self.queries[topology]:
                        relation_data = []
                        for enum_name in target_enums:
                            query_costs = self.cost_data[enum_name][topology][relations]
                            query_opt_costs = self.cost_data["dpccp"][topology][relations]
                            relation_data.append([])
                            for q_idx in query_costs:
                                if query_costs[q_idx] < query_opt_costs[q_idx]:
                                    print(["more optimal query cost", enum_name, topology, relations, q_idx])
                                relation_data[-1].append(query_costs[q_idx] / query_opt_costs[q_idx])

                        df_temp = pd.DataFrame(relation_data).T
                        df_temp.columns=target_enums_print
                        df_temp = df_temp.assign(Trial=relations)
                        all_dfs.append(df_temp)

                    font_size_value = 14  
                    plt.rcParams["figure.figsize"] = (10,10)
                    palette_colors = ['brown', 'grey', 'pink', 'orange', 'purple', 'blue']
                    cdf = pd.concat(all_dfs)
                    mdf = pd.melt(cdf, id_vars=['Trial'], var_name=['Enumerators'])
                    ax = sns.boxplot(x="Trial", y="value", hue="Enumerators", fliersize=3, data=mdf, palette=palette_colors)

                    ax.set_ylim([0.9, 4])
                    if t_idx == 0:
                        ax.legend(fontsize=font_size_value, loc='upper left')
                        ax.set_ylabel("Normalized Plan Cost", fontsize=font_size_value, fontdict=dict(weight='bold'))
                    elif t_idx == 2: # for arXiv version
                        ax.set_ylabel("Normalized Plan Cost", fontsize=font_size_value, fontdict=dict(weight='bold'))
                        ax.legend([], [], frameon=False)
                    else: 
                        ax.set_ylabel("", fontsize=font_size_value, fontdict=dict(weight='bold'))
                        ax.set_yticklabels([])
                        ax.legend([], [], frameon=False)
                    ax.set_xlabel("# relations", fontsize=font_size_value, fontdict=dict(weight='bold'))
                    # ax.set_title(topology_names[topology], fontsize=font_size_value, fontdict=dict(weight='bold'))

                    plt.savefig(figure_file_name + topology_names[topology] + ".pdf", bbox_inches='tight')
                    plt.close()

        GenerateFigures()

        ####################################
        
        print("\nSuccess.\n")
    except:
        print("Script errors.\n")
