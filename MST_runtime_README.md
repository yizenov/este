<meta name="robots" content="noindex,nofollow">

# Instructions to execute query plans selected by the enumeration algorithms.

- Required: `python3`, and `statistics` and `screen` packages.
- Each script includes instructions how to run with input arguments. Each script can be run using true and estimated cardinalities. In addition, each script can be run with `screen` which its instructions are provided within each python script.
- Installed PostgreSQL docker container. Follow the instructions from [here](PostgreSQL_README.md).
- Installed [PG_HINT](http://pghintplan.osdn.jp).
- Create indexes. The list of indexes are provided, [PK-indexes](input_data/indexes_pk_11.sql) and [FK-indexes](input_data/indexes_fk_24.sql).


## Table of Contents
1. [Part 1](#generate_query). Generate query plans
2. [Part 2](#execute_plans). Execute query plans
3. [Part 3](#parse_runtime). Parsing runtime results
4. [Part 4](#ensemble_mst_runtime). Generate Ensemble MST runtime


## 1. Generate query plans <a name="generate_query"></a>
Script file: `scripts_runtime/generate_physical_plans.py`
Input file: 
- `input_data/job/workload_queries`
- `output_data/job/costs`
Output file: 
- `input_data/job/PHYSICAL_PLANS/TRUE_PLANS`
- `input_data/job/PHYSICAL_PLANS/EST_PLANS`

[Query plans](output_data/job/costs/), obtained from the enumeration algorithms, are generated in .sql files at `input_data/job/PHYSICAL_PLANS/`. The SQL files can be generated from query plans selected when using true and estimated cardinalities. Their corresponding SQL files are generated in `TRUE_PLANS` and `EST_PLANS`. All queries with `EXPLAIN ANALYZE` and their physical operators and join orders are forced using [pg_hint](http://pghintplan.osdn.jp).

Enumeration algorithms:
- `Exhaustive`
- `GOO`
- `Prim's` and `Prim's` from each node
- `Kruskal's` and `Kruskal's` from each node

## 2. Execute query plans <a name="execute_plans"></a>
Script file: `scripts_runtime/benchmark_psql_runtime.sh`
Input file: 
- `input_data/job/PHYSICAL_PLANS/TRUE_PLANS`
- `input_data/job/PHYSICAL_PLANS/EST_PLANS`
Output file: 
- `output_data/job/PHYSICAL_PLANS/TRUE_PLANS`
- `output_data/job/PHYSICAL_PLANS/EST_PLANS`

Create corresponding output folders. Each query is run `5 times` and median is considered in the results. `Timing` and `ph_hint` are enabled. The docker container name is `mst_pg_docker` and database name is `imdb`. The script can be run with `screen` which its instructions are provided within the script.

## 3. Parsing runtime results <a name="parse_runtime"></a>
Script file: `scripts_runtime/parse_runtime.py`
Input file: 
- `input_data/job/workload_queries`
- `output_data/job/PHYSICAL_PLANS/TRUE_PLANS`
- `output_data/job/PHYSICAL_PLANS/EST_PLANS`
Output file: 
- `output_data/job/runtime`

- The output of each enumeration algorithm is a csv file stored in [here](output_data/job/runtime/) that includes, for each query, PostgreSQL optimization time, execution time, and total runtime. The script parses each query logs that run 5 times. Their corresponding log files are stored in `TRUE_PLANS` and `EST_PLANS`.

Enumeration algorithms:
- `Exhaustive`
- `GOO`
- `Prim's` and `Prim's` from each node
- `Kruskal's` and `Kruskal's` from each node

## 4. Generate Ensemble MST runtime <a name="ensemble_mst_runtime"></a>
Script file: `scripts_runtime/parse_ensemble_runtime.py`
Input file: 
- `input_data/job/workload_queries`

- `output_data/job/costs/kruskal_ensemble_opt_plans.csv`
- `output_data/job/costs/prim_ensemble_opt_plans.csv`
- `output_data/job/costs/kruskal_ensemble_opt_plans_psql.csv`
- `output_data/job/costs/prim_ensemble_opt_plans_psql.csv`

- `output_data/job/runtime/enum_run_job_kruskal_ensemble.csv`
- `output_data/job/runtime/enum_run_job_prim_ensemble.csv`
- `output_data/job/runtime/enum_run_job_kruskal_ensemble_psql.csv`
- `output_data/job/runtime/enum_run_job_prim_ensemble_psql.csv`
Output file: 
- `output_data/job/runtime/enum_run_job_ensemble.csv`
- `output_data/job/runtime/enum_run_job_ensemble_psql.csv`

This script does not parse the workload queries logs. It uses the output generated in `kruskal_ensemble` and `prim_ensemble`. Ensemble MST chooses the plan with minimum cost and and its corresponding execution time of Prim's and Kruskal's run from each node. No need to run this script with `screen`, it is quick.
