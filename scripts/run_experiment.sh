#!/bin/bash

experiment=$1
query=$2
protocol=$3
interval=$4
rate=$5
saving_dir=$6
failure=$7
graph_id=$8
resource_id=$9
intelligent_allocation=${10}
interval_active=${11}
ratio_active=${12}
accuracy=${13}
enable_cascadefailure=${14}
AF_Tolerance=${15}

array=()

cd ../../TATA
if [[ "$intelligent_allocation" == "deep_force" ]]; then
  output=$(python inference.py --graph_id $graph_id --resource_id $resource_id --distribution 0)
elif [[ "$intelligent_allocation" == "storm" ]]; then
  output=$(python inference.py --graph_id $graph_id --resource_id $resource_id --distribution 1)
else
  output=$(python inference.py --graph_id $graph_id --resource_id $resource_id --distribution 2)
fi
echo $output
cd ../checkmate/scripts
IFS=',' read -r -a array <<< "$output"
scale_factor=$(( ${#array[@]} / 2 ))

bash deploy_run.sh "$protocol" "$interval" "$failure" "$scale_factor" "$enable_cascadefailure" "$AF_Tolerance" "${array[@]}"


sleep 10
if [[ $query == "q1" || $query == "q2" ]]; then
    python ../queries/nexmark_queries/"$query"/nexmark-client.py -r "$rate" -bp "$scale_factor" -t "$interval_active" -rt "$ratio_active" -acc "$accuracy" -af "$AF_Tolerance"
elif [[ $query == "q3" ]]; then
    python ../queries/nexmark_queries/"$query"/nexmark-client.py -r "$rate" -pp "$scale_factor" -ap "$scale_factor" -t "$interval_active" -rt "$ratio_active" -acc "$accuracy" -af"$AF_Tolerance"
fi

echo "generator exited"
mkdir -p "$saving_dir"/"$experiment"/figures

echo " 开始 kafka input和 output"
python ../queries/nexmark_queries/"$query"/kafka_input_consumer.py "$saving_dir" "$experiment"
python ../queries/nexmark_queries/"$query"/kafka_output_consumer.py "$saving_dir" "$experiment"
echo " 结束 kafka input和 output"
echo "------------------------------开始计算指标----------------------------------------------"
python ../queries/nexmark_queries/"$query"/metrics/calculate_latency.py "$saving_dir" "$experiment"

bash delete_deployment.sh "$experiment" "$saving_dir"