import math
import pandas as pd
import numpy as np

input_msgs = pd.read_csv('kafka_input.csv', usecols=['request_id', 'request'])
output_msgs = pd.read_csv('kafka_output.csv', usecols=['request_id', 'response'])

joined = pd.merge(input_msgs, output_msgs, on='request_id', how='outer')
joined.to_csv('kafka_merged_result.csv', index=False, encoding='utf-8')
