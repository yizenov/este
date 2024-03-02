<meta name="robots" content="noindex,nofollow">

# MST Evaluation
We provide instructions to reproduce each figure and table in the paper.

- Required: `python3`.
- Each script includes instructions how to run with input arguments.

## Table of Contents
1. [Part 1](#cost_opt_data). Generate C_mm cost and optimization time data
2. [Part 2](#exec_data). Generate execution time data
3. [Figure 8, 9 and 10](#figure789). Update .ods files for Figures 8, 9, and 10
4. [Tables 1 and 2](#tables12). Update Tables 1 and 2
5. [Figure 11 and 12](#topology). Generate C_out cost data optimization time data and figures
6. [Figure 7](#plan_space_cover). Generate plan search space coverage data

## 1. Generate C_mm cost and optimization time data <a name="cost_opt_data"></a>
Script file: `figures/generate_cost_figure_data.py`
Input file: 
- `input_data/job/workload_queries`
- `output_data/job/costs/`
Output file: 
- `figures/figures_data/figure_7a_data.csv`
- `figures/figures_data/figure_7b_data.csv`
- `figures/figures_data/figure_8a_data.csv`
- `figures/figures_data/figure_8b_data.csv`

- `figures/figures_data/tables_1_cost_data.csv`
- `figures/figures_data/tables_2_cost_data.csv`
- `figures/figures_data/tables_1_opt_data.csv`
- `figures/figures_data/tables_2_opt_data.csv`

## 2. Generate execution time data <a name="exec_data"></a>
Script file: `figures/generate_runtime_figure_data.py`
Input file: 
- `input_data/job/workload_queries`
- `output_data/job/runtime/`
Output file: 
- `figures/figures_data/figure_9a_data.csv`
- `figures/figures_data/figure_9b_data.csv`
- `figures/figures_data/figure_10a_data.csv`
- `figures/figures_data/figure_10b_data.csv`

- `figures/figures_data/tables_1_exec_data.csv`
- `figures/figures_data/tables_2_exec_data.csv`
- `figures/figures_data/tables_1_psql_opt_data.csv`
- `figures/figures_data/tables_2_psql_opt_data.csv`

Execution time is converted from milliseconds to seconds. Optimization time from PostgreSQL logs are not used since we force join order and physical operators.

## 3. Update .ods files for Figures 8, 9, and 10 <a name="figure789"></a>
Figure ods files:
- Figure 8 (a, b) `figures/job_plan_costs.ods`
- Figure 9 (a, b) `figures/job_plan_optimization_time.ods`
- Figure 10 (a, b) `figures/job_plan_execution_time.ods`

Copy figure data, inside csv files from `figures/figures_data/`, to these ods files. The figures are automatically updated.

## 4. Update Tables 1 and 2 <a name="tables12"></a>
Table 1 and 2 from the paper can be directly compared with the table data from `figures/figures_data/`.

## 5. Generate C_out cost data optimization time data and figures <a name="topology"></a>
Script file:
- `figures/generate_cost_cout_boxplots.py`
- `figures/generate_opt_cout_boxplots.py`
Input file:
- `input_data/topology/workload_queries`
- `output_data/topology/costs_cout/`
Output file:
- `figures/topology_cout_box_plots_chain.pdf`
- `figures/topology_cout_box_plots_cycle.pdf`
- `figures/topology_cout_box_plots_star.pdf`
- `figures/topology_cout_box_plots_clique.pdf`

- `figures/topology_opt_box_plots_chain.pdf`
- `figures/topology_opt_box_plots_cycle.pdf`
- `figures/topology_opt_box_plots_star.pdf`
- `figures/topology_opt_box_plots_clique.pdf`

## 6. Generate plan search space coverage data <a name="plan_space_cover"></a>
Script file: `figures/generate_coverage_figures_data.py`
Input file:
- `input_data/job/workload_queries`

- `output_data/job/plans_subplans/prim_ensemble_all_plans.csv`
- `output_data/job/plans_subplans/kruskal_ensemble_all_plans.csv`
- `output_data/job/plans_subplans/prim_ensemble_sub_plans.csv`
- `output_data/job/plans_subplans/kruskal_ensemble_sub_plans.csv`

- `output_data/job/plans_subplans/prim_ensemble_all_plans_psql.csv`
- `output_data/job/plans_subplans/kruskal_ensemble_all_plans_psql.csv`
- `output_data/job/plans_subplans/prim_ensemble_sub_plans_psql.csv`
- `output_data/job/plans_subplans/kruskal_ensemble_sub_plans_psql.csv`
Output file:
- `figures/figures_data/figure_coverage_subplan_data_true.csv`
- `figures/figures_data/figure_coverage_plan_data_true.csv`

- `figures/figures_data/figure_coverage_subplan_data_est.csv`
- `figures/figures_data/figure_coverage_plan_data_est.csv`

Copy figure data, inside csv files from `figures/figures_data/`, to `job_plan_coverage` ods file. The figure is automatically updated.
