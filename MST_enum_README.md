<meta name="robots" content="noindex,nofollow">

# Instructions to run each enumeration algorithm from the paper.

- Required: `python3`, and `heapq` and `screen` packages.
- Each script includes instructions how to run with input arguments. Each script can be run using true and estimated cardinalities. In addition, each script can be run with `screen` which its instructions are provided within each python script.
- The output of each algorithm is a csv file stored in [here](output_data/job/costs/) that includes, for each query, optimization time, costs, selected edge sequences, and join operators.
- The input for the algorithms is the workload queries stored in `input_data/job/workload_queries`. We skip family 29 queries because of the non-termination in the case of exhaustive enumeration.
- Enumeration algorithms use the same cost function from the paper implemented in `scripts_cost/cost_module.py`. The input to this cost model is cardinalities (`input_data/job/results_estimates-JOB.csv`).


## Table of Contents
1. [Part 1](#prim_enum). Run Prim's enumeration algorithm
2. [Part 2](#prim_ensemble_enum). Run Prim's enumeration algorithm from each node
3. [Part 3](#kruskal_enum). Run Kruskal's enumeration algorithm
4. [Part 4](#kruskal_ensemble_enum). Run Kruskal's enumeration algorithm from each node
5. [Part 5](#ensemble_mst_enum). Generate Ensemble MST
6. [Part 6](#goo_enum). Run GOO enumeration algorithm
7. [Part 7](#exhaustive_enum). Run Exhaustive enumeration algorithm


## 1. Run Prim's enumeration algorithm <a name="prim_enum"></a>
Script file: `scripts_cost/mst_prim.py`
Output file: 
- `output_data/job/costs/prim_opt_plans.csv`
- `output_data/job/costs/prim_opt_plans_psql.csv`

## 2. Run Prim's enumeration algorithm from each node <a name="prim_ensemble_enum"></a>
Script file: `scripts_cost/mst_prim_ensemble.py`
Output file: 
- `output_data/job/costs/prim_ensemble_opt_plans.csv`
- `output_data/job/costs/prim_ensemble_opt_plans_psql.csv`

## 3. Run Kruskal's enumeration algorithm <a name="kruskal_enum"></a>
Script file: `scripts_cost/mst_kruskal.py`
Output file: 
- `output_data/job/costs/kruskal_opt_plans.csv`
- `output_data/job/costs/kruskal_opt_plans_psql.csv`

## 4. Run Kruskal's enumeration algorithm from each node <a name="kruskal_ensemble_enum"></a>
Script file: `scripts_cost/mst_kruskal_ensemble.py`
Output file: 
- `output_data/job/costs/kruskal_ensemble_opt_plans.csv`
- `output_data/job/costs/kruskal_ensemble_opt_plans_psql.csv`

## 5. Generate Ensemble MST <a name="ensemble_mst_enum"></a>
Script file: `scripts_cost/mst_generate_ensemble.py`
Output file: 
- `output_data/job/costs/ensemble_opt_plans.csv`
- `output_data/job/costs/ensemble_opt_plans_psql.csv`

This script does not run the workload queries. It uses the output generated from [Part 2](#prim_ensemble_enum) and [Part 4](#kruskal_ensemble_enum). Ensemble MST chooses the plan with minimum cost and sums optimization time of Prim's and Kruskal's run from each node. No need to run this script with `screen`, it is quick.

## 6. Run GOO enumeration algorithm <a name="goo_enum"></a>
Script file: `scripts_cost/mst_goo.py`
Output file: 
- `output_data/job/costs/goo_opt_plans.csv`
- `output_data/job/costs/goo_opt_plans_psql.csv`

## 7. Run Exhaustive enumeration algorithm <a name="exhaustive_enum"></a>
Script file: `scripts_cost/mst_exhaustive.py`
Output file: 
- `output_data/job/costs/exhaustive_opt_plans.csv`
- `output_data/job/costs/exhaustive_opt_plans_psql.csv`
