import json
import sys
import pandas as pd
import numpy as np
import math
import matplotlib.pyplot as plt

saving_dir = sys.argv[1]
experiment_name = sys.argv[2]

input_msgs = pd.read_csv(f'{saving_dir}/{experiment_name}/{experiment_name}-input.csv')
output_msgs = pd.read_csv(f'{saving_dir}/{experiment_name}/{experiment_name}-output.csv')
experiment_length = 60 # in seconds
joined = pd.merge(input_msgs, output_msgs, on='request_id', how='outer')
responded = joined.dropna().sort_values('timestamp_x').reset_index()
# responded = responded[responded['timestamp_x'] > (30000 + responded['timestamp_x'][0])].reset_index()

runtime = responded['timestamp_y'] - responded['timestamp_x']

runtime_df = pd.DataFrame(runtime, columns=['latency_ms'])
runtime_df.to_csv(f'{saving_dir}/{experiment_name}/runtime_latency.csv', index=False)


print(f'end_to_end_latency : {np.percentile(runtime, 50)}ms')
print("--------------------------------------------------------------------------------")
print(f'min latency: {min(runtime)}ms')
print(f'max latency: {max(runtime)}ms')
print(f'average latency: {np.average(runtime)}ms')
print(f'99%: {np.percentile(runtime, 99)}ms')
print(f'95%: {np.percentile(runtime, 95)}ms')
print(f'90%: {np.percentile(runtime, 90)}ms')
print(f'75%: {np.percentile(runtime, 75)}ms')
print(f'60%: {np.percentile(runtime, 60)}ms')
print(f'50%: {np.percentile(runtime, 50)}ms')
print(f'25%: {np.percentile(runtime, 25)}ms')
print(f'10%: {np.percentile(runtime, 10)}ms')

input_sorted = input_msgs.sort_values("timestamp").reset_index() 
output_sorted = output_msgs.sort_values("timestamp").reset_index() 
total_messages = len(input_msgs.index)
total_time = (output_sorted["timestamp"].iloc[-1] - input_sorted["timestamp"].iloc[0])/1_000
print(f'average_throughput: {total_messages/total_time}')
