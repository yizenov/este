
cardinality_job_file = "input_data/job/results_estimates-JOB.csv"

TABLE_SCAN_COST = 0.2  # 1.0, 0.2
INDEX_COST = 2.0  # 1.0, 2.0

ZERO_VAL = 0.01  # replacing zero in denominators

class CostFunctions(object):
    def __init__(self, arg_table_nicks, arg_input_query, arg_selectivities):
        self.TABLE_SCAN_COST = TABLE_SCAN_COST
        self.INDEX_COST = INDEX_COST

        self.table_nicks = arg_table_nicks
        self.input_query = arg_input_query

        self.all_cardinalities = {}

        self.goo_cardinalities = {}
        self.selectivities = arg_selectivities

        self.load_cardinalities()

        self.global_subplan_set = set()
        self.global_join_set = set()

    def load_cardinalities(self):
        with open(cardinality_job_file, "r") as input_f:
            for idx, line in enumerate(input_f):
                if idx == 0: continue
                line = line.strip().split(",")

                query, join_size = line[0].split("_")[0], int(line[1])
                subquery, true_card, psql_card = line[0], int(line[2]), float(line[3])

                subplan = ",".join(line[4:])[2:-2].split(",")
                subplan = " ".join(sorted([node.strip()[1:-1].split("-")[1] for node in subplan]))

                if query not in self.all_cardinalities: self.all_cardinalities[query] = {}
                self.all_cardinalities[query][subplan] = [subquery, join_size, true_card, psql_card]

    def compute_c_mm_cost(self, left_tables, right_tables, edge_pair):
        l, r = len(left_tables), len(right_tables)
        left_subplan = " ".join(sorted(left_tables))
        right_subplan = " ".join(sorted(right_tables))
        current_subplan = " ".join(sorted(left_tables + right_tables))

        if (left_subplan, right_subplan) not in self.global_join_set \
            and (right_subplan, left_subplan) not in self.global_join_set:
                self.global_join_set.add((left_subplan, right_subplan))
        if current_subplan not in self.global_subplan_set:
            self.global_subplan_set.add(current_subplan)

        ######################## Reading Cardinalities #####################################
        # NOTE: assume base table has true cardinality, even with selection predicate
        # NOTE: assume base table sizes are stored in catalogs

        left_card_true = self.all_cardinalities[self.input_query][left_subplan][2]
        left_card_psql = self.all_cardinalities[self.input_query][left_subplan][3]
        if self.selectivities: # NOTE: GOO
            goo_left_card_true = self.goo_cardinalities[left_subplan][0]
            goo_left_card_psql = self.goo_cardinalities[left_subplan][1]

        right_card_true = self.all_cardinalities[self.input_query][right_subplan][2]
        right_card_psql = self.all_cardinalities[self.input_query][right_subplan][3]
        if self.selectivities: # NOTE: GOO
            goo_right_card_true = self.goo_cardinalities[right_subplan][0]
            goo_right_card_psql = self.goo_cardinalities[right_subplan][1]

        ######################## Side Selections #####################################
        # NOTE: HJ -- zig-zag tree structure (probing, hash build)
        # NOTE: INL -- outer/inner table (scan side and index side)

        current_card_true = self.all_cardinalities[self.input_query][current_subplan][2]  # true
        current_card_psql = self.all_cardinalities[self.input_query][current_subplan][3]  # psql

        if left_card_true < right_card_true:
            hash_build_side_true = left_card_true
            hash_join_true_info = ["HJ", right_subplan, left_subplan]
            outer_table_true = right_card_true
            inl_join_true_info = ["INL", right_subplan, left_subplan]
        else:
            hash_build_side_true = right_card_true
            hash_join_true_info = ["HJ", left_subplan, right_subplan]
            outer_table_true = left_card_true
            inl_join_true_info = ["INL", left_subplan, right_subplan]

        if left_card_psql < right_card_psql:
            hash_build_side_psql = left_card_psql
            hash_join_psql_info = ["HJ", right_subplan, left_subplan]
            outer_table_psql = right_card_psql
            inl_join_psql_info = ["INL", right_subplan, left_subplan]
        else:
            hash_build_side_psql = right_card_psql
            hash_join_psql_info = ["HJ", left_subplan, right_subplan]
            outer_table_psql = right_card_psql
            inl_join_psql_info = ["INL", left_subplan, right_subplan]

        if self.selectivities: # NOTE: GOO
            goo_current_card_true = goo_left_card_true * goo_right_card_true * edge_pair[0]
            goo_current_card_psql = goo_left_card_psql * goo_right_card_psql * edge_pair[1]
            self.goo_cardinalities[current_subplan] = [goo_current_card_true, goo_current_card_psql]

            if goo_left_card_true < goo_right_card_true:
                goo_hash_build_side_true = goo_left_card_true
                goo_hash_join_true_info = ["HJ", right_subplan, left_subplan]
                goo_outer_table_true = goo_right_card_true
                goo_inl_join_true_info = ["INL", right_subplan, left_subplan]
            else:
                goo_hash_build_side_true = goo_right_card_true
                goo_hash_join_true_info = ["HJ", left_subplan, right_subplan]
                goo_outer_table_true = goo_left_card_true
                goo_inl_join_true_info = ["INL", left_subplan, right_subplan]

            if goo_left_card_psql < goo_right_card_psql:
                goo_hash_build_side_psql = goo_left_card_psql
                goo_hash_join_psql_info = ["HJ", right_subplan, left_subplan]
                goo_outer_table_psql = goo_right_card_psql
                goo_inl_join_psql_info = ["INL", right_subplan, left_subplan]
            else:
                goo_hash_build_side_psql = goo_right_card_psql
                goo_hash_join_psql_info = ["HJ", left_subplan, right_subplan]
                goo_outer_table_psql = goo_left_card_psql
                goo_inl_join_psql_info = ["INL", left_subplan, right_subplan]

        ######################## Join Operator Costs #####################################
        # NOTE: coefficients for base and intermediate table to distinguish from index scan

        if l != 1 and r != 1: # bushy join case
            hash_join_true = current_card_true + hash_build_side_true
            hash_join_psql = current_card_psql + hash_build_side_psql
            index_nested_loop_join_true = None  # NOTE: no index in inner tables, only base table
            index_nested_loop_join_psql = None
            if self.selectivities: # NOTE: GOO
                goo_hash_join_true = goo_current_card_true + goo_hash_build_side_true
                goo_hash_join_psql = goo_current_card_psql + goo_hash_build_side_psql
                goo_index_nested_loop_join_true = None  # NOTE: no index in inner tables, only base table
                goo_index_nested_loop_join_psql = None
        elif l == 1 and r == 1: # both are base tables
            hash_join_true = current_card_true + hash_build_side_true \
                + self.TABLE_SCAN_COST * (left_card_true + right_card_true)
            hash_join_psql = current_card_psql + hash_build_side_psql \
                + self.TABLE_SCAN_COST * (left_card_psql + right_card_psql)
            index_nested_loop_join_true = self.TABLE_SCAN_COST * outer_table_true \
                + self.INDEX_COST * outer_table_true * max(current_card_true / max(outer_table_true, ZERO_VAL), 1)
            index_nested_loop_join_psql = self.TABLE_SCAN_COST * outer_table_psql \
                + self.INDEX_COST * outer_table_psql * max(current_card_psql / max(outer_table_psql, ZERO_VAL), 1)
            if self.selectivities: # NOTE: GOO
                goo_hash_join_true = goo_current_card_true + goo_hash_build_side_true \
                    + self.TABLE_SCAN_COST * (goo_left_card_true + goo_right_card_true)
                goo_hash_join_psql = goo_current_card_psql + goo_hash_build_side_psql \
                    + self.TABLE_SCAN_COST * (goo_left_card_psql + goo_right_card_psql)
                goo_index_nested_loop_join_true = self.TABLE_SCAN_COST * goo_outer_table_true \
                    + self.INDEX_COST * goo_outer_table_true * max(goo_current_card_true / max(goo_outer_table_true, ZERO_VAL), 1)
                goo_index_nested_loop_join_psql = self.TABLE_SCAN_COST * goo_outer_table_psql \
                    + self.INDEX_COST * goo_outer_table_psql * max(goo_current_card_psql / max(goo_outer_table_psql, ZERO_VAL), 1)
        elif l == 1: # left-deep
            hash_join_true = current_card_true + hash_build_side_true + self.TABLE_SCAN_COST * left_card_true
            hash_join_psql = current_card_psql + hash_build_side_psql + self.TABLE_SCAN_COST * left_card_psql
            index_nested_loop_join_true = self.INDEX_COST * right_card_true * max(current_card_true / max(right_card_true, ZERO_VAL), 1)
            index_nested_loop_join_psql = self.INDEX_COST * right_card_psql * max(current_card_psql / max(right_card_psql, ZERO_VAL), 1)
            if self.selectivities: # NOTE: GOO
                goo_hash_join_true = goo_current_card_true + goo_hash_build_side_true \
                    + self.TABLE_SCAN_COST * goo_left_card_true
                goo_hash_join_psql = goo_current_card_psql + goo_hash_build_side_psql \
                    + self.TABLE_SCAN_COST * goo_left_card_psql
                goo_index_nested_loop_join_true = self.INDEX_COST * goo_right_card_true \
                    * max(goo_current_card_true / max(goo_right_card_true, ZERO_VAL), 1)
                goo_index_nested_loop_join_psql = self.INDEX_COST * goo_right_card_psql \
                    * max(goo_current_card_psql / max(goo_right_card_psql, ZERO_VAL), 1)
        elif r == 1: # left-deep
            hash_join_true = current_card_true + hash_build_side_true + self.TABLE_SCAN_COST * right_card_true
            hash_join_psql = current_card_psql + hash_build_side_psql + self.TABLE_SCAN_COST * right_card_psql
            index_nested_loop_join_true = self.INDEX_COST * left_card_true * max(current_card_true / max(left_card_true, ZERO_VAL), 1)
            index_nested_loop_join_psql = self.INDEX_COST * left_card_psql * max(current_card_psql / max(left_card_psql, ZERO_VAL), 1)
            if self.selectivities: # NOTE: GOO
                goo_hash_join_true = goo_current_card_true + goo_hash_build_side_true \
                    + self.TABLE_SCAN_COST * goo_right_card_true
                goo_hash_join_psql = goo_current_card_psql + goo_hash_build_side_psql \
                    + self.TABLE_SCAN_COST * goo_right_card_psql
                goo_index_nested_loop_join_true = self.INDEX_COST * goo_left_card_true \
                    * max(goo_current_card_true / max(goo_left_card_true, ZERO_VAL), 1)
                goo_index_nested_loop_join_psql = self.INDEX_COST * goo_left_card_psql \
                    * max(goo_current_card_psql / max(goo_left_card_psql, ZERO_VAL), 1)

        ######################## Join Operator Selection #####################################

        if index_nested_loop_join_true is None or index_nested_loop_join_true > hash_join_true:
            join_cost_true = hash_join_true
            join_true_info = hash_join_true_info
        else:
            join_cost_true = index_nested_loop_join_true
            join_true_info = inl_join_true_info
        
        if index_nested_loop_join_psql is None or index_nested_loop_join_psql > hash_join_psql:
            join_cost_psql = hash_join_psql
            join_psql_info = hash_join_psql_info
        else:
            join_cost_psql = index_nested_loop_join_psql
            join_psql_info = inl_join_psql_info

        if self.selectivities: # NOTE: GOO
            if goo_index_nested_loop_join_true is None or goo_index_nested_loop_join_true > goo_hash_join_true:
                goo_join_cost_true = goo_hash_join_true
                goo_join_true_info = goo_hash_join_true_info
            else:
                goo_join_cost_true = goo_index_nested_loop_join_true
                goo_join_true_info = goo_inl_join_true_info

            if goo_index_nested_loop_join_psql is None or goo_index_nested_loop_join_psql > goo_hash_join_psql:
                goo_join_cost_psql = goo_hash_join_psql
                goo_join_psql_info = goo_hash_join_psql_info
            else:
                goo_join_cost_psql = goo_index_nested_loop_join_psql
                goo_join_psql_info = goo_inl_join_psql_info

        #############################################################################

        subquery_id = self.all_cardinalities[self.input_query][current_subplan][0]
        join_size = self.all_cardinalities[self.input_query][current_subplan][1]
        results = [join_cost_true, join_cost_psql]
        results_info = [join_true_info, join_psql_info]

        if self.selectivities: # NOTE: GOO
            goo_results = [goo_join_cost_true, goo_join_cost_psql]
            goo_results_info = [goo_join_true_info, goo_join_psql_info]
        else: goo_results, goo_results_info = None, None

        return [subquery_id, join_size, results, goo_results, results_info, goo_results_info]
    