import asyncio
import logging
import random
import subprocess
import time

import uvloop
from universalis.common.stateflow_ingress import IngressTypes
from universalis.nexmark.config import config
from universalis.universalis import Universalis

from operators.bids_source import bids_source_operator
from operators.currency_mapper import currency_mapper_operator
from operators.sink import sink_operator
from operators.q1_graph import g

UNIVERSALIS_HOST: str = 'localhost'
UNIVERSALIS_PORT: int = 8886
KAFKA_URL = 'localhost:9093'


async def main():
    args = config()
    universalis = Universalis(UNIVERSALIS_HOST, UNIVERSALIS_PORT,
                              ingress_type=IngressTypes.KAFKA,
                              kafka_url=KAFKA_URL)
    await universalis.start()

    channel_list = [
        (None, 'bidsSource', False),
        ('bidsSource', 'currencyMapper', False),
        ('currencyMapper', 'sink', False),
        ('sink', None, False)
    ]

    await universalis.send_channel_list(channel_list)

    await asyncio.sleep(5)

    ####################################################################################################################
    # SUBMIT STATEFLOW GRAPH ###########################################################################################
    ####################################################################################################################
    scale = int(args.bids_partitions)
    bids_source_operator.set_partitions(scale)
    currency_mapper_operator.set_partitions(scale)
    sink_operator.set_partitions(scale)
    g.add_operators(bids_source_operator, currency_mapper_operator, sink_operator)
    await universalis.submit(g)

    print('Graph submitted')
    time.sleep(20)

    import os
    jar_path = os.path.expanduser("../nexmark/target/nexmark-generator-1.0-SNAPSHOT-jar-with-dependencies.jar")

    # input("Press when you want to start producing.")
    print("启动nexmark数据生成器")
    subprocess.call(
        ["java", "-jar", jar_path,
         "--generator-parallelism", "1",
         "--enable-bids-topic", "true",
         "--load-pattern", "cosine",
         "--experiment-length", "1",
         "--use-default-configuration", "false",  
         "--cosine-period", "10",  # 余弦波动周期（分钟）-----
         "--input-rate-mean", str(args.rate),  # 平均输入速率---
         "--input-rate-maximum-divergence", "0",  # 输入速率的最大偏差----
         "--max-noise", "0",  # 最大噪声
         "--iteration-duration-ms", "90000",
         "--kafka-server", "localhost:9093",
         "--uni-bids-partitions", str(args.bids_partitions)],
    )
    print("结束nexmark数据生成过程")
    await universalis.close()


uvloop.install()
asyncio.run(main())
print("nexmark-client.py 执行完了")


