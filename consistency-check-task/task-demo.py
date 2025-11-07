import asyncio
import random
import time
import json
import pandas as pd
from timeit import default_timer as timer


import uvloop
from universalis.common.stateflow_ingress import IngressTypes
from universalis.universalis import Universalis

from operators.graph import g
from operators.graph import source_operator
from operators.graph import task_operator
from operators.graph import sink_operator

messages_per_second = 800
sleeps_per_second = 100
sleep_time = 0.00085

UNIVERSALIS_HOST: str = 'localhost'
UNIVERSALIS_PORT: int = 8886
KAFKA_URL = 'localhost:9093'


async def main():
    universalis = Universalis(UNIVERSALIS_HOST, UNIVERSALIS_PORT,
                              ingress_type=IngressTypes.KAFKA,
                              kafka_url=KAFKA_URL)
    await universalis.start()

    channel_list = [
        (None, 'source', False),
        ('source', 'task', True),
        ('task', 'sink', True),
        ('sink', None, False)
    ]

    await universalis.send_channel_list(channel_list)

    operators = {element for item in channel_list for element in item[:2] if element is not None}
    print(list(operators))
    scale = len(operators)
    source_operator.set_partitions(scale)
    task_operator.set_partitions(scale)
    sink_operator.set_partitions(scale)
    g.add_operators(source_operator, task_operator, sink_operator)
    await universalis.submit(g)

    print('Graph submitted')

    time.sleep(2)

    key = list(operators).index('sink')
    with open("../self_task/config.json") as f:
        data = json.load(f)
    samples = list()
    for sample in data["positive_samples"]:
        samples.append(sample)
    for sample in data["negative_samples"]:
        samples.append(sample)

    tasks = []
    for _ in range(5):
        sec_start = timer()
        for i in range(messages_per_second):
            if i % (messages_per_second // sleeps_per_second) == 0:
                await asyncio.gather(*tasks)
                tasks = []
                time.sleep(sleep_time)
            random_value = random.randint(0, 3)
            tasks.append(universalis.send_kafka_event(operator=source_operator,
                                                      key=key,
                                                      function='read',
                                                      params=(operators, samples[random_value],)))
        await asyncio.gather(*tasks)
        tasks = []
    print('Run done, waiting...')
    time.sleep(8)

    await universalis.close()

uvloop.install()
asyncio.run(main())
