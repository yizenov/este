#!/bin/bash

# RUN FROM 'mst_query_optimization' folder
# screen -S doc_psql_true_exh -dm -L -Logfile scr_run_true_exh.0 sh -c 'time scripts_runtime/benchmark_psql_runtime.sh'
# screen -S doc_psql_est_exh -dm -L -Logfile scr_run_est_exh.0 sh -c 'time scripts_runtime/benchmark_psql_runtime.sh'


container_name=mst_pg_docker

input_folder=input_data/job/PHYSICAL_PLANS
output_folder=output_data/job/PHYSICAL_PLANS

# input_folder=${input_folder}/TRUE_PLANS
# output_folder=${output_folder}/TRUE_PLANS
input_folder=${input_folder}/EST_PLANS
output_folder=${output_folder}/EST_PLANS

# input_folder=${input_folder}/prim
# input_folder=${input_folder}/prim_ensemble
# input_folder=${input_folder}/kruskal
# input_folder=${input_folder}/kruskal_ensemble
# input_folder=${input_folder}/goo
input_folder=${input_folder}/exhaustive

# output_folder=${output_folder}/prim
# output_folder=${output_folder}/prim_ensemble
# output_folder=${output_folder}/kruskal
# output_folder=${output_folder}/kruskal_ensemble
# output_folder=${output_folder}/goo
output_folder=${output_folder}/exhaustive


iterations=5
timing_enabled="\timing"
pg_hint_enabled="LOAD 'pg_hint_plan';"

docker_server_run="docker exec -i ${container_name} psql -h localhost -U postgres -d imdb"

echo "starting the query runs: " -- `date +"%Y-%m-%d %T"`
echo -e "\n"
for query_file in $(ls ${input_folder})
do
  echo ${query_file} -- `date +"%Y-%m-%d %T"`
  query_result=${output_folder}/${query_file}.result
  rm ${query_result}
  touch ${query_result}

  client_query=${output_folder}/${query_file}
  rm ${client_query}
  touch ${client_query}
  echo "${pg_hint_enabled} " > ${client_query}
  echo "${timing_enabled} " >> ${client_query}
  cat "${input_folder}/${query_file}" >> ${client_query}
  chmod +x ${client_query}

  iter=1
  while [ $iter -le ${iterations} ]
  do
    (${docker_server_run} < ${client_query} >> ${query_result}) & P0=$!
    wait $P0
    ((iter++))
  done;

done;

echo -e "\nfinishing the query runs: " -- `date +"%Y-%m-%d %T"`
