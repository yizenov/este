import os, sys

# generating left-deep and bushy plans with fixed order and physical operators

primary_key_indexes = {
    "title" : [("id", "pk_title")],
    "name" : [("id", "pk_name")],
    "info_type" : [("id", "pk_info_type")],
    "comp_cast_type" : [("id", "pk_comp_cast_type")],
    "char_name" : [("id", "pk_char_name")],
    "role_type" : [("id", "pk_role_type")],
    "link_type" : [("id", "pk_link_type")],
    "company_name" : [("id", "pk_company_name")],
    "company_type" : [("id", "pk_company_type")],
    "kind_type" : [("id", "pk_kind_type")],
    "keyword" : [("id", "pk_keyword")]
}

foreign_key_indexes = {
    "cast_info" : [("person_role_id", "fk_person_role_id_cast_info"), ("role_id", "fk_role_id_cast_info"), ("person_id", "fk_person_id_cast_info"), ("movie_id", "fk_movie_id_cast_info")],
    "complete_cast" : [("subject_id", "fk_subject_id_complete_cast"), ("status_id", "fk_status_id_complete_cast"), ("movie_id", "fk_movie_id_complete_cast")],
    "movie_link" : [("linked_movie_id", "fk_linked_movie_id_movie_link"), ("link_type_id", "fk_link_type_id_movie_link"), ("movie_id", "fk_movie_id_movie_link")],
    "movie_companies" : [("company_id", "fk_company_id_movie_companies"), ("company_type_id", "fk_company_type_id_movie_companies"), ("movie_id", "fk_movie_id_movie_companies")],
    "title" : [("kind_id", "fk_kind_id_title")],
    "aka_name" : [("person_id", "fk_person_id_aka_name")],
    "aka_title" : [("movie_id", "fk_movie_id_aka_title")],
    "movie_keyword" : [("keyword_id", "fk_keyword_id_movie_keyword"), ("movie_id", "fk_movie_id_movie_keyword")],
    "person_info" : [("person_id", "fk_person_id_person_info"), ("info_type_id", "fk_info_type_id_person_info")],
    "movie_info" : [("info_type_id", "fk_info_type_id_movie_info"), ("movie_id", "fk_movie_id_movie_info")],
    "movie_info_idx" : [("info_type_id", "fk_info_type_id_movie_info_idx"), ("movie_id", "fk_movie_id_movie_info_idx")]
}

def find_index(enum, query, table_name, column_name):
    if table_name in primary_key_indexes:
        for pair in primary_key_indexes[table_name]:
            if pair[0] == column_name:
                return pair[1]
    elif table_name in foreign_key_indexes:
        for pair in foreign_key_indexes[table_name]:
            if pair[0] == column_name:
                return pair[1]
    
    print(["index not found", enum, query, table_name, column_name])
    return None

def find_preserved_order(tables_list, new_table, insert_type, orders_set):
    temp = tables_list.split()
    for idx, set_tables in enumerate(orders_set):
        if len(set_tables[0]) == len(temp):
            found = True
            for t in set_tables[0]:
                if t not in temp:
                    found = False
                    break
            if found:
                set_tables_str = " ".join(set_tables[0])
                if insert_type == 0: set_tables[0].insert(0, new_table)
                elif insert_type == 1: set_tables[0].append(new_table)
                return set_tables_str, idx

# NOTE: building the leading (join order) hint and operator hints
#   not possible: cycles, missing nodes
def next_hint(left, right, curr_order_sets, join_type, curr_query):

    if len(left.split()) == 1 and len(right.split()) == 1: # new component
        curr_lead_hint = "(" + left + " " + right + ")"
        new_hint_str = "\t" + join_type + "(" + left + " " + right + ")\n"

        curr_order_sets.append(([left, right], curr_lead_hint))
    elif len(left.split()) == 1: # left-deep
        right_order, order_idx = find_preserved_order(right, left, 0, curr_order_sets)
        new_hint_str = "\t" + join_type + "(" + left + " " + right_order + ")\n"

        curr_lead_hint = curr_order_sets[order_idx][1]
        curr_lead_hint = "(" + left + " " + curr_lead_hint + ")"
        curr_order_sets[order_idx] = (curr_order_sets[order_idx][0], curr_lead_hint)
    elif len(right.split()) == 1: # right-deep
        left_order, order_idx = find_preserved_order(left, right, 1, curr_order_sets)
        new_hint_str = "\t" + join_type + "(" + left_order + " " + right + ")\n"

        curr_lead_hint = curr_order_sets[order_idx][1]
        curr_lead_hint = "(" + curr_lead_hint + " " + right + ")"
        curr_order_sets[order_idx] = (curr_order_sets[order_idx][0], curr_lead_hint)
    else: # bushy, merge sub-plans
        left_order, left_order_idx = find_preserved_order(left, None, 2, curr_order_sets)
        right_order, right_order_idx = find_preserved_order(right, None, 2, curr_order_sets)

        left_lead_hint = curr_order_sets[left_order_idx][1]
        right_lead_hint = curr_order_sets[right_order_idx][1]
        curr_lead_hint = "(" + left_lead_hint + " " + right_lead_hint + ")"
        new_hint_str = "\t" + join_type + "(" + left_order + " " + right_order + ")\n"

        new_order = left_order.split() + right_order.split()
        curr_order_sets.append((new_order, curr_lead_hint))

        if right_order_idx > left_order_idx:
            curr_order_sets.pop(right_order_idx)
            curr_order_sets.pop(left_order_idx)
        else:
            curr_order_sets.pop(left_order_idx)
            curr_order_sets.pop(right_order_idx)
    return new_hint_str

print(
"\n \
1. Enter: ~/mst_query_optimization\n \
2. Run the following command: /usr/bin/python3 scripts_runtime/generate_physical_plans.py arg1\n \
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
            enumerations = ["exhaustive", "goo", "kruskal_ensemble", "kruskal", "prim_ensemble", "prim"]

            pg_prefix = "EXPLAIN ANALYZE "
            # pg_prefix = "EXPLAIN "
            # pg_prefix = ""

            # job benchmark
            input_queries = "input_data/job/workload_queries"
            input_plans_folder = "output_data/job/costs/"

            output_folder = "input_data/job/PHYSICAL_PLANS/"
            os.makedirs(output_folder, exist_ok = True)
            if cardinality_idx == 0: 
                output_folder += "TRUE_PLANS/"
                os.makedirs(output_folder, exist_ok = True)
            else: 
                output_folder += "EST_PLANS/"
                os.makedirs(output_folder, exist_ok = True)

            ############################ Query Predicates #################################################
            all_queries, input_query_files = {}, sorted(os.listdir(input_queries))
            for idx, file_name in enumerate(input_query_files):
                query_name = file_name.split("_")[1][:-4]

                input_query = input_queries + "/" + file_name
                with open(input_query, "r") as query_input_f:

                    original_query = [query_line for query_line in query_input_f]
                    original_query = "".join(original_query)

                    # extracting tables and join predicates
                    from_and_where = original_query.split('FROM')[1].split('WHERE')
                    table_list = from_and_where[0].split(',')
                    table_list = [table.strip().split(" AS ") for table in table_list]
                    table_list = {table_info[1]: table_info[0] for table_info in table_list}

                    # collecting tables and join predicates information
                    where_clause = from_and_where[1].split('\n\n')
                    where_clause = [clause_set for clause_set in where_clause if clause_set]

                    filter_predicates = where_clause[0].split('\n')
                    filter_predicates = [cond.strip() for cond in filter_predicates if cond.strip()]
                    filter_predicates = [cond[4:] if c_idx > 0 else cond for c_idx, cond in enumerate(filter_predicates)]
                    filter_predicates = {cond.split(".")[0]: cond for cond in filter_predicates}
                    
                    join_preds = where_clause[1].split('\n')
                    join_preds = [join.strip() for join in join_preds if join.strip()]
                    join_preds[-1] = join_preds[-1][:-1]
                    join_preds = [cond[4:] for cond in join_preds]

                    join_predicates = {}
                    for cond in join_preds:
                        cond_both = cond.split(" = ")
                        l_table, l_pred = cond_both[0].split(".")
                        r_table, r_pred = cond_both[1].split(".")
                        join_predicates[(l_table, r_table)] = (l_pred, r_pred)
                        join_predicates[(r_table, l_table)] = (r_pred, l_pred)
                    all_queries[query_name] = [table_list, join_predicates]

            ############################ Selected Plans #################################################

            all_selected_plans = []
            for enum_idx, enum_method in enumerate(enumerations):
                os.makedirs(output_folder + enum_method, exist_ok = True)
                all_selected_plans.append({})

                plans_file_name = input_plans_folder + enum_method
                if cardinality_idx == 0: plans_file_name += "_opt_plans.csv"
                else: plans_file_name += "_opt_plans_psql.csv"

                with open(plans_file_name, "r") as input_f:
                    for idx, line in enumerate(input_f):
                        if idx == 0: continue
                        line = line.strip().split(",")

                        query, opt_time = line[0], line[1]
                        est_cost, true_cost = line[2], line[3]
                        plan_edges = [edge.split("-") for edge in line[4].strip().split()]

                        all_operators, order_sets = [], []
                        hint_str, hash_operators = "/*+\n", line[5].strip().split("HJ-")
                        for opts in hash_operators:
                            opts = opts.strip()

                            if opts == "": continue
                            elif "INL-" not in opts: 
                                opts = opts.split("-")
                                if len(opts[0].split()) == 1: hint_str += "\tSeqScan(" + opts[0] + ")\n"
                                if len(opts[1].split()) == 1: hint_str += "\tSeqScan(" + opts[1] + ")\n"
                                hint_str += next_hint(opts[0], opts[1], order_sets, "HashJoin", query)
                                all_operators.append(["HJ", opts[0], opts[1]])
                            else:
                                loop_operators = opts.split("INL-")
                                for inl_idx, inl in enumerate(loop_operators):
                                    inl = inl.strip()
                                    if inl == "": continue

                                    inl = inl.split("-")
                                    if inl_idx == 0: 
                                        if len(inl[0].split()) == 1: hint_str += "\tSeqScan(" + inl[0] + ")\n"
                                        if len(inl[1].split()) == 1: hint_str += "\tSeqScan(" + inl[1] + ")\n"
                                        hint_str += next_hint(inl[0], inl[1], order_sets, "HashJoin", query)
                                        all_operators.append(["HJ", inl[0], inl[1]])
                                    else: 
                                        if len(inl[0].split()) == 1:
                                            outer_table, inner_table = inl[1], inl[0]
                                        elif len(inl[1].split()) == 1:
                                            outer_table, inner_table = inl[0], inl[1]
                                        else: outer_table, inner_table = None, None

                                        # find and create index scan
                                        index_name = ""
                                        for adj in outer_table.split():
                                            if (inner_table, adj) in all_queries[query][1]:
                                                column_name = all_queries[query][1][(inner_table, adj)][0]
                                                table_name = all_queries[query][0][inner_table]
                                                index_name = find_index(enum_method, query, table_name, column_name)
                                            elif (adj, inner_table) in all_queries[query][1]:
                                                column_name = all_queries[query][1][(adj, inner_table)][1]
                                                table_name = all_queries[query][0][inner_table]
                                                index_name = find_index(enum_method, query, table_name, column_name)
                                        hint_str += "\tIndexScan(" + inner_table + " " + index_name + ")\n"

                                        hint_str += next_hint(outer_table, inner_table, order_sets, "NestLoop", query)
                                        all_operators.append(["INL", outer_table, inner_table])
                        if len(plan_edges) != len(all_operators):
                            print(["missing operators", enum_method, query])

                        lead_hint = order_sets[0][1]
                        if len(order_sets) != 1: print("wrong order construction")

                        hint_str += "\n\tLeading(" + lead_hint + ")\n"
                        hint_str += "*/"
                        all_selected_plans[enum_idx][query] = [opt_time, est_cost, true_cost, plan_edges, hint_str]

            ############################ Generate Join Order ############################################

            def build_plan_sql(arg_query, edges, tables, join_pred, filters):
                all_components, is_bushy, sql_parts = [], False, {}

                for edge in edges:
                    left, right = False, False
                    left_idx, right_idx = -1, -1
                    for c_idx, component in enumerate(all_components):
                        if edge[0] in component:
                            if left == True: print("wrong component left: " + arg_query)
                            left_idx, left = c_idx, True
                        if edge[1] in component:
                            if right == True: print("wrong component right: " + arg_query)
                            right_idx, right = c_idx, True

                    if left and right and left_idx == right_idx:  # cycle
                        print("cycle detected: " + arg_query)
                        continue
                    elif not left and not right:  # new component
                        all_components.append(set())
                        all_components[-1].add(edge[0])
                        all_components[-1].add(edge[1])
                        subplan = " ".join(sorted(all_components[-1]))

                        sql_sub_query = "(" + tables[edge[0]] + " AS " + edge[0] + " JOIN " + tables[edge[1]] + " AS " + edge[1] + " ON (" + join_pred[edge[0] + "," + edge[1]]
                        if edge[0] in filters: sql_sub_query += " AND " + filters[edge[0]]
                        if edge[1] in filters: sql_sub_query += " AND " + filters[edge[1]]
                        sql_sub_query += "))"

                        sql_parts[subplan] = sql_sub_query

                        join_pred.pop(edge[0] + "," + edge[1])
                        join_pred.pop(edge[1] + "," + edge[0])
                        if edge[0] in filters: filters.pop(edge[0])
                        if edge[1] in filters: filters.pop(edge[1])

                    elif left and not right:  # left-deep
                        curr_subplan = " ".join(sorted(all_components[left_idx]))
                        sql_sub_query = sql_parts[curr_subplan]
                        sql_sub_query = sql_sub_query[:-1] + " JOIN " + tables[edge[1]] + " AS " + edge[1] + " ON ("
                        
                        # NOTE: several connections possible and postgres decides which edge to use
                        for node in all_components[left_idx]:
                            temp_edge = node + "," + edge[1]
                            if temp_edge in join_pred:
                                sql_sub_query += join_pred[temp_edge] + " AND "
                                join_pred.pop(node + "," + edge[1])
                                join_pred.pop(edge[1] + "," + node)
                        sql_sub_query = sql_sub_query[:-5]

                        if edge[1] in filters: sql_sub_query += " AND " + filters[edge[1]]
                        sql_sub_query += "))"

                        all_components[left_idx].add(edge[1])
                        subplan = " ".join(sorted(all_components[left_idx]))
                        sql_parts[subplan] = sql_sub_query
                        sql_parts.pop(curr_subplan)

                        if edge[1] in filters: filters.pop(edge[1])

                    elif not left and right:  # right-deep
                        curr_subplan = " ".join(sorted(all_components[right_idx]))
                        sql_sub_query = sql_parts[curr_subplan]
                        sql_sub_query = sql_sub_query[:-1] + " JOIN " + tables[edge[0]] + " AS " + edge[0] + " ON ("
                        
                        # NOTE: several connections possible and postgres decides which edge to use
                        for node in all_components[right_idx]:
                            temp_edge = node + "," + edge[0]
                            if temp_edge in join_pred:
                                sql_sub_query += join_pred[temp_edge] + " AND "
                                join_pred.pop(node + "," + edge[0])
                                join_pred.pop(edge[0] + "," + node)
                        sql_sub_query = sql_sub_query[:-5]

                        if edge[0] in filters: sql_sub_query += " AND " + filters[edge[0]]
                        sql_sub_query += "))"

                        all_components[right_idx].add(edge[0])
                        subplan = " ".join(sorted(all_components[right_idx]))
                        sql_parts[subplan] = sql_sub_query
                        sql_parts.pop(curr_subplan)

                        if edge[0] in filters: filters.pop(edge[0])

                    elif left and right and left_idx != right_idx:  # merge sub-plans
                        is_bushy = True
                        curr_left_subplan = " ".join(sorted(all_components[left_idx]))
                        sql_left_sub_query = sql_parts[curr_left_subplan]

                        curr_right_subplan = " ".join(sorted(all_components[right_idx]))
                        sql_right_sub_query = sql_parts[curr_right_subplan]

                        # NOTE: several connections possible and postgres decides which edge to use
                        sql_sub_query = "(" + sql_left_sub_query + " JOIN " + sql_right_sub_query + " ON ("
                        for l_node in all_components[left_idx]:
                            for r_node in all_components[right_idx]:
                                temp_edge = l_node + "," + r_node
                                if temp_edge in join_pred:
                                    sql_sub_query += join_pred[temp_edge] + " AND "
                                    join_pred.pop(l_node + "," + r_node)
                                    join_pred.pop(r_node + "," + l_node)
                        sql_sub_query = sql_sub_query[:-5]
                        sql_sub_query += "))"

                        [all_components[left_idx].add(n) for n in all_components[right_idx]]
                        subplan = " ".join(sorted(all_components[left_idx]))
                        all_components.pop(right_idx)
                        sql_parts[subplan] = sql_sub_query

                        sql_parts.pop(curr_left_subplan)
                        sql_parts.pop(curr_right_subplan)

                if len(all_components) != 1: print("multiple final components: " + arg_query)
                final_query = list(sql_parts)[-1]
                sql_final_query = sql_parts[final_query]
                new_query = "SELECT COUNT(*) \nFROM " + sql_final_query + ";\n"

                parenthesis_left_count, parenthesis_right_count = 0, 0
                for i in range(len(new_query)):
                    if new_query[i] == "(": parenthesis_left_count += 1
                    if new_query[i] == ")": parenthesis_right_count += 1
                if parenthesis_left_count - parenthesis_right_count != 0: print("wrong parethesis: " + arg_query)
                return new_query

            ############################ All Queries ###################################################

            input_query_files = sorted(os.listdir(input_queries))
            for idx, file_name in enumerate(input_query_files):
                query_name = file_name.split("_")[1][:-4]

                input_query = input_queries + "/" + file_name
                with open(input_query, "r") as query_input_f:

                    original_query = [query_line for query_line in query_input_f]
                    original_query = "".join(original_query)

                    # extracting tables and join predicates
                    from_and_where = original_query.split('FROM')[1].split('WHERE')
                    table_list = from_and_where[0].split(',')
                    table_list = [table.strip().split(" AS ") for table in table_list]
                    table_list = {table_info[1]: table_info[0] for table_info in table_list}

                    # collecting tables and join predicates information
                    where_clause = from_and_where[1].split('\n\n')
                    where_clause = [clause_set for clause_set in where_clause if clause_set]

                    filter_predicates = where_clause[0].split('\n')
                    filter_predicates = [cond.strip() for cond in filter_predicates if cond.strip()]
                    filter_predicates = [cond[4:] if c_idx > 0 else cond for c_idx, cond in enumerate(filter_predicates)]
                    filter_predicates = {cond.split(".")[0]: cond for cond in filter_predicates}
                    
                    join_preds = where_clause[1].split('\n')
                    join_preds = [join.strip() for join in join_preds if join.strip()]
                    join_preds[-1] = join_preds[-1][:-1]
                    join_preds = [cond[4:] for cond in join_preds]

                    join_predicates = {}
                    for cond in join_preds:
                        cond_both = cond.split(" = ")
                        cond_key_v1 = cond_both[0].split(".")[0] + "," + cond_both[1].split(".")[0]
                        cond_key_v2 = cond_both[1].split(".")[0] + "," + cond_both[0].split(".")[0]
                        join_predicates[cond_key_v1] = cond
                        join_predicates[cond_key_v2] = cond
                    
                    for enum_idx, enum_method in enumerate(enumerations):
                        if query_name not in all_selected_plans[enum_idx]:
                            print(["missing plan", enum_method, query_name])
                            continue

                        query_plan_edges = all_selected_plans[enum_idx][query_name][3]
                        hint_str = all_selected_plans[enum_idx][query_name][4]

                        # new_query = build_plan_sql(query_name, query_plan_edges, table_list, 
                        #                            join_predicates.copy(), filter_predicates.copy())
                        
                        # NOTE: original_query or query with fixed order (new_query)
                        new_query = hint_str + "\n" + pg_prefix + original_query

                        output_f = open(output_folder + enum_method + "/fixed_order_" + query_name + ".sql", "w")
                        output_f.write(new_query)
                        output_f.close()

            ####################################
            
            print("\nSuccess.\n")
    except:
        print("Script errors.\n")
