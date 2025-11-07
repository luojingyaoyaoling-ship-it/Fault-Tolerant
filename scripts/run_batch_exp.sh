#!/bin/bash

input=../csv_files/"$1.csv"
saving_dir=../save_res

echo $input

while IFS= read -r line
do
  printf 'Run experiment: %s\n' "$line"
  IFS=',' read -ra ss <<< "$line"
  exp_name="${ss[0]}"
  query="${ss[1]}"
  protocol="${ss[2]}"
  interval="${ss[3]}"
  rate="${ss[4]}"
  failure="${ss[5]}"
  graph_id="${ss[6]}"
  resource_id="${ss[7]}"
  intelligent_allocation="${ss[8]}"
  interval_active="${ss[9]}"
  ratio_active="${ss[10]}"
  accuracy="${ss[11]}"
  enable_Cascadefailure="${ss[12]}"
  AF_Tolerance="${ss[13]}"



  bash run_experiment.sh "$exp_name" "$query" "$protocol" "$interval" "$rate" "$saving_dir" \
                              "$failure" "$graph_id" "$resource_id" "$intelligent_allocation" "$interval_active" "$ratio_active" "$accuracy" "$enable_Cascadefailure" "$AF_Tolerance"
done < "$input"
