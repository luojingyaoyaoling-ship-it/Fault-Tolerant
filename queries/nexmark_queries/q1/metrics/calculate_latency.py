import sys
import pandas as pd
import numpy as np

saving_dir = sys.argv[1]
experiment_name = sys.argv[2]
# protocol = sys.argv[3]

input_msgs = pd.read_csv(f'{saving_dir}/{experiment_name}/{experiment_name}-input.csv')
output_msgs = pd.read_csv(f'{saving_dir}/{experiment_name}/{experiment_name}-output.csv')
experiment_length= 60 # in seconds
joined = pd.merge(input_msgs, output_msgs, on='request_id', how='outer')
joined_sorted = joined.sort_values('timestamp_x').reset_index()
joined_sorted = joined_sorted[joined_sorted['timestamp_x'] > (30000 + joined_sorted['timestamp_x'][0])].reset_index()
runtime = joined_sorted['timestamp_y'] - joined_sorted['timestamp_x']
runtime_no_nan = runtime.dropna()

print(f'end_to_end_latency : {np.percentile(runtime_no_nan, 50)}ms')
 

input_sorted = input_msgs.sort_values("timestamp").reset_index() 
output_sorted = output_msgs.sort_values("timestamp").reset_index() 
total_messages = len(input_msgs.index)
total_time = (output_sorted["timestamp"].iloc[-1] - input_sorted["timestamp"].iloc[0])/1_000
print(f'average_throughput: {total_messages/total_time}')
