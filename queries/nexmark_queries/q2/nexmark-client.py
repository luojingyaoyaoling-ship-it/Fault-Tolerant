import asyncio
import subprocess
import time

import uvloop
from universalis.common.stateflow_ingress import IngressTypes
from universalis.universalis import Universalis

from operators.bids_source import bids_source_operator
from operators.sink import sink_operator
from operators.count import count_operator
# from checkmate.queries.nexmark_queries.q2.operators import q2_graph
from operators import q2_graph

UNIVERSALIS_HOST: str = 'localhost'
UNIVERSALIS_PORT: int = 8886
KAFKA_URL = 'localhost:9093'


def config():
    import argparse
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-r", "--rate",
                            help="Provide the input rate.",
                            default="1000",
                            type=str,
                            action="store")
    arg_parser.add_argument("-bp", "--bids_partitions",
                            help="Provide the number of bids topic partitions.",
                            default="5",
                            type=str,
                            action="store")
    arg_parser.add_argument("-pp", "--persons_partitions",
                            help="Provide the number of persons topic partitions.",
                            default="5",
                            type=str,
                            action="store")
    arg_parser.add_argument("-ap", "--auctions_partitions",
                            help="Provide the number of auctions topic partitions.",
                            default="5",
                            type=str,
                            action="store")
    arg_parser.add_argument("-s", "--skew",
                            help="Turn on skew.",
                            default="0",
                            type=str,
                            action="store")
    arg_parser.add_argument("-t", "--interval",
                            help="change or not",
                            default="0",
                            type=str,
                            action="store")
    arg_parser.add_argument("-rt", "--ratio",
                            help="change or not",
                            default="0",
                            type=str,
                            action="store")
    arg_parser.add_argument("-acc", "--accuracy",
                            default="0.0",
                            type=str,
                            action="store")
    arguments = arg_parser.parse_args()

    return arguments


async def main():
    args = config()

    universalis = Universalis(UNIVERSALIS_HOST, UNIVERSALIS_PORT,
                              ingress_type=IngressTypes.KAFKA,
                              kafka_url=KAFKA_URL)
    await universalis.start()

    channel_list = [
        (None, 'bidsSource', False),
        ('bidsSource', 'count', True),
        ('count', 'sink', False),
        ('sink', None, False)
    ]

    await universalis.send_channel_list(channel_list)

    ####################################################################################################################
    # SUBMIT STATEFLOW GRAPH ###########################################################################################
    ####################################################################################################################
    scale = int(args.bids_partitions)
    bids_source_operator.set_partitions(scale)
    count_operator.set_partitions(scale)
    sink_operator.set_partitions(scale)
    q2_graph.g.add_operators(bids_source_operator, count_operator, sink_operator)
    await universalis.submit(q2_graph.g)

    print('Graph submitted')

    time.sleep(60)

    tasks = []
    for key in range(scale):
        tasks.append(universalis.send_kafka_event(operator=count_operator,
                                                  key=key,
                                                  function="trigger",
                                                  params=(10,)
                                                  ))
    responses = await asyncio.gather(*tasks)
    print(responses)
    tasks = []
    import os
    jar_path = os.path.expanduser("../nexmark/target/nexmark-generator-1.0-SNAPSHOT-jar-with-dependencies.jar")
    subprocess.call(["java", "-jar", jar_path,
                     "--query", "1",
                     "--generator-parallelism", "1",
                     "--enable-bids-topic", "true",
                     "--load-pattern", "static",
                     "--experiment-length", "1",
                     "--use-default-configuration", "false",
                     "--rate", args.rate,
                     "--max-noise", "0",
                     "--iteration-duration-ms", "90000",
                     "--kafka-server", "localhost:9093",
                     "--uni-bids-partitions", args.bids_partitions,
                     "--enable_dynamic_interval", str(args.interval),
                     "--enable_dynamic_ratio", str(args.ratio)
                     ])

    await universalis.close()


uvloop.install()
asyncio.run(main())
