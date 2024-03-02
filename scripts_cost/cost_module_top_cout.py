
cardinality_job_file = "input_data/topology/results_estimates-topology.csv"

TABLE_SCAN_COST = 0.2  # 1.0, 0.2
INDEX_COST = 2.0  # 1.0, 2.0

ZERO_VAL = 0.01  # replacing zero in denominators

def load_cardinalities():
    all_cardinalities = {}
    with open(cardinality_job_file, "r") as input_f:
        for idx, line in enumerate(input_f):
            if idx == 0: continue
            line = line.strip().split(",")
 
            query, join_size = "_".join(line[0].split("_")[:-1]), int(line[1])
            subquery, true_card, psql_card = line[0], int(line[2]), float(line[3])
            
            if join_size == 1: subplan = ",".join(line[4:])[1:-1].split(",")
            else: subplan = ",".join(line[4:])[2:-2].split(",")
            subplan = " ".join(sorted([node.strip()[1:-1].split("-")[1] for node in subplan]))

            if query not in all_cardinalities: all_cardinalities[query] = {}
            all_cardinalities[query][subplan] = [subquery, join_size, true_card, psql_card]
    return all_cardinalities

class CostFunctionsTopCout(object):
    def __init__(self, arg_table_nicks, arg_input_query, arg_selectivities, arg_all_cardinalities):
        self.TABLE_SCAN_COST = TABLE_SCAN_COST
        self.INDEX_COST = INDEX_COST

        self.table_nicks = arg_table_nicks
        self.input_query = arg_input_query

        self.all_cardinalities = arg_all_cardinalities

        self.goo_cardinalities = {}
        self.selectivities = arg_selectivities

    def compute_c_mm_cost(self, left_tables, right_tables, edge_pair):
        l, r = len(left_tables), len(right_tables)
        current_subplan = " ".join(sorted(left_tables + right_tables))

        ######################## Side Selections #####################################

        current_card_true = self.all_cardinalities[self.input_query][current_subplan][2]  # true
        current_card_psql = self.all_cardinalities[self.input_query][current_subplan][3]  # psql

        ######################## Join Operator Costs #####################################
        # NOTE: coefficients for base and intermediate table to distinguish from index scan

        if l != 1 and r != 1: # bushy join case
            hash_join_true = current_card_true
            hash_join_psql = current_card_psql
        elif l == 1 and r == 1: # both are base tables
            hash_join_true = current_card_true
            hash_join_psql = current_card_psql
        elif l == 1: # left-deep
            hash_join_true = current_card_true
            hash_join_psql = current_card_psql
        elif r == 1: # left-deep
            hash_join_true = current_card_true
            hash_join_psql = current_card_psql

        ######################## Join Operator Selection #####################################

        join_cost_true = hash_join_true
        join_cost_psql = hash_join_psql

        #############################################################################

        subquery_id = self.all_cardinalities[self.input_query][current_subplan][0]
        join_size = self.all_cardinalities[self.input_query][current_subplan][1]
        results = [join_cost_true, join_cost_psql]
        results_info = [None, None]
        goo_results, goo_results_info = None, None

        return [subquery_id, join_size, results, goo_results, results_info, goo_results_info]
    