import asyncio
import random
import subprocess
import numpy as np
import time
import json
import uvloop
from universalis.common.stateflow_ingress import IngressTypes
from universalis.universalis import Universalis
from operators.sink import sink_operator
from operators.auctions_source import auctions_source_operator
from operators.join_operator import join_operator
from operators.persons_filter import persons_filter_operator
from operators.persons_source import persons_source_operator
from operators import q3_graph
import math
import random
UNIVERSALIS_HOST: str = 'localhost'
UNIVERSALIS_PORT: int = 8886
KAFKA_URL = 'localhost:9093'

def calculate_optimal_checkpoint_interval(predicted_rate, max_rate=6000, original_interval=5):
    """
    根据预测速率和最大处理速率计算最佳检查点间隔。
    :param predicted_rate: 预测的输入速率
    :param max_rate: 最大处理速率（默认值 6000）
    :param original_interval: 原始检查点间隔（默认值 5）
    :return: 最佳检查点间隔（在 5 附近）
    """
    # 计算预测速率与最大处理速率的比例
    rate_ratio = predicted_rate / max_rate

    # 使用数学变换动态调整检查点间隔
    # 1. 使用对数变换调整比例
    log_adjustment = math.log(1 + rate_ratio)  # log(1 + x) 确保结果为正
    # 2. 使用平方根变换进一步平滑
    sqrt_adjustment = math.sqrt(log_adjustment)
    # 3. 将调整因子应用到原始检查点间隔
    adjustment_factor = 1 + (sqrt_adjustment - 0.5) * 0.2  # 调整因子范围动态变化
    checkpoint_interval = original_interval * adjustment_factor

    # 引入随机扰动：通过平方函数引入随机性
    random_perturbation = 1 + (random.random() - 0.5) ** 2  # 平方函数使扰动更平滑
    checkpoint_interval *= random_perturbation
    checkpoint_interval += 3
    return checkpoint_interval


# 模拟一个简单的预测模型（移动平均）

def gen_backup_ratio_acc(predicted_rate, channel_list, operator_scales, accu=0.7):
    """
    根据预测速度、Channel List、Operator Scale 和准确度要求生成备份率。
    :param predicted_rate: 预测的速度
    :param channel_list: Channel List，包含通道信息
    :param operator_scales: 每个 Operator 的分区数（scale）
    :param accu: 要求的准确度（0-1之间，默认0.7）
    :return: 备份率（介于 0.7 到 0.99 之间）
    """
    # 1. 速度的影响（使用 Sigmoid 函数）
    normalized_rate = (predicted_rate - 3000) / 3000  # 将 predicted_rate 归一化到 [-1, 1]
    speed_score = 1 / (1 + math.exp(-normalized_rate))  # Sigmoid 函数

    # 2. Channel List 的影响（遍历通道列表）
    channel_score = 0
    num_channels = len(channel_list)
    channel_connections = {}  # 记录每个 Operator 的连接数

    # 遍历 channel_list，统计每个 Operator 的连接数
    for channel in channel_list:
        source, target, _ = channel
        if source not in channel_connections:
            channel_connections[source] = 0
        if target not in channel_connections:
            channel_connections[target] = 0
        channel_connections[source] += 1
        channel_connections[target] += 1

    # 计算 Channel List 的复杂度评分
    total_connections = sum(channel_connections.values())
    channel_score = (num_channels * total_connections) / 100  # 归一化到合理范围

    # 3. Operator Scale 的影响（遍历分区数）
    total_scale = sum(operator_scales)
    avg_scale = total_scale / len(operator_scales) if operator_scales else 1
    scale_score = 1 / (1 + avg_scale / 10)  # 分区数越多，score 越低

    # 4. 准确度影响因子（准确度要求越高，备份率越高）
    # 将准确度要求映射到 [0.8, 1.2] 区间
    accu_factor = 0.8 + (accu - 0.5) * 2  # 假设accu在0.5-1.0之间

    # 5. 综合评分（加权平均）
    combined_score = (
        0.4 * speed_score +      # 速度影响 40%
        0.3 * channel_score +    # Channel List 影响 30%
        0.2 * scale_score +      # Operator Scale 影响 20%
        0.1 * accu_factor        # 准确度要求影响 10%
    )

    # 6. 非线性变换到 [0.7, 0.99] 区间
    # 使用 Sigmoid 函数进行非线性变换
    sigmoid_score = 1 / (1 + math.exp(-(combined_score - 0.5) * 10))
    
    # 基础备份率范围 [0.7, 0.99]
    base_backup_ratio = 0.7 + 0.29 * sigmoid_score
    
    # 根据准确度要求进一步调整
    # 准确度每提高0.1，备份率增加0.05（最大不超过0.99）
    accu_adjusted_ratio = min(base_backup_ratio + (accu - 0.7) * 0.5, 0.99)
    
    return max(0.7, min(accu_adjusted_ratio, 0.99))  # 确保在[0.7, 0.99]范围内

def gen_backup_ratio_no_acc(predicted_rate, channel_list, operator_scales):
    """
    根据预测速度、Channel List 和 Operator Scale 生成一个介于 0.7 到 0.99 之间的备份率。
    :param predicted_rate: 预测的速度
    :param channel_list: Channel List，包含通道信息
    :param operator_scales: 每个 Operator 的分区数（scale）
    :return: 备份率（介于 0.7 到 0.99 之间）
    """
    # 1. 速度的影响（使用 Sigmoid 函数）
    normalized_rate = (predicted_rate - 3000) / 3000  # 将 predicted_rate 归一化到 [-1, 1]
    speed_score = 1 / (1 + math.exp(-normalized_rate))  # Sigmoid 函数

    # 2. Channel List 的影响（遍历通道列表）
    channel_score = 0
    num_channels = len(channel_list)
    channel_connections = {}  # 记录每个 Operator 的连接数

    # 遍历 channel_list，统计每个 Operator 的连接数
    for channel in channel_list:
        source, target, _ = channel
        if source not in channel_connections:
            channel_connections[source] = 0
        if target not in channel_connections:
            channel_connections[target] = 0
        channel_connections[source] += 1
        channel_connections[target] += 1

    # 计算 Channel List 的复杂度评分
    # 复杂度与通道数量和连接数成正比
    total_connections = sum(channel_connections.values())
    channel_score = (num_channels * total_connections) / 100  # 归一化到合理范围

    # 3. Operator Scale 的影响（遍历分区数）
    total_scale = sum(operator_scales)
    avg_scale = total_scale / len(operator_scales) if operator_scales else 1
    scale_score = 1 / (1 + avg_scale / 10)  # 分区数越多，score 越低

    # 4. 综合评分（加权平均）
    combined_score = (
        0.5 * speed_score +  # 速度影响 50%
        0.3 * channel_score +  # Channel List 影响 30%
        0.2 * scale_score  # Operator Scale 影响 20%
    )

    # 5. 非线性变换到 [0.7, 0.99] 区间
    # 使用 Sigmoid 函数进行非线性变换
    sigmoid_score = 1 / (1 + math.exp(-(combined_score - 0.5) * 10))  # 调整 Sigmoid 曲线的陡峭度
    backup_ratio = 0.7 + 0.29 * sigmoid_score  # 映射到 [0.7, 0.99]

    return backup_ratio


    

async def monitor_and_predict(args, channel_list):
    start_time = time.time()  # 记录开始时间
    # operators_set = set()

    # for src, dest, _ in channel_list:
    #     if src is not None:
    #         operators_set.add(src)
    #     if dest is not None:
    #         operators_set.add(dest)

    # operators_num: int = len(operators_set)

    while True:

        # 检查是否已经监控了 半分钟（30 秒）
        if time.time() - start_time >= 30:
            with open("../dynamic_params/predict_result.json") as f0:
                data = json.load(f0)
                predicted_rate = data["predictRate"]

            # 生成备份率
            # 使用 persons_partitions 和 auctions_partitions 的平均值作为 operator_scales
            avg_partitions = (int(args.persons_partitions) + int(args.auctions_partitions)) // 2
            operator_scales = [avg_partitions] * 5  # 假设每个 Operator 的分区数相同
            
            if args.interval == "1":
                start_timex = time.time()
                checkpoint_interval = calculate_optimal_checkpoint_interval(predicted_rate, int(args.rate))
                elapsed_time_ms = (time.time() - start_timex)
                adjust_time = data["elapsedTime"] + elapsed_time_ms
                print(f"Function execution time: {adjust_time:.2f} ms")
                
                print(f"Optimal checkpoint interval: {checkpoint_interval}")
                with open("../dynamic_params/checkpoint_interval.json", "w") as f1:
                    json.dump({"checkpoint_interval": math.ceil(checkpoint_interval)}, f1, indent=4)

            if args.ratio == "1":
                if args.accuracy == "0.0":
                    backup_ratio = gen_backup_ratio_no_acc(predicted_rate, channel_list, operator_scales)
                else:
                    acc = float(args.accuracy)
                    backup_ratio = gen_backup_ratio_acc(predicted_rate, channel_list, operator_scales, acc)
                print(f"Predicted average rate for next 20 seconds: {predicted_rate}")
                print(f"Generated backup ratio: {backup_ratio}")
                with open("../dynamic_params/ratio.json", "w") as f:
                    json.dump({"ratio": round(backup_ratio, 2)}, f, indent=4)

            # 重置监控
            # historical_rates = []  # 清空历史数据
            # start_time = time.time()  # 重置开始时间
            break

def init_data():
    with open("../dynamic_params/ratio.json", "w") as f:
        json.dump({"ratio": 0.9}, f, indent=4)

    with open("../dynamic_params/checkpoint_interval.json", "w") as f1:
        json.dump({"checkpoint_interval": 5}, f1, indent=4)

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

    
async def ensure_kafka_topics_exist(partitions_persons: int, partitions_auctions: int):
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_URL)
    topics_to_create = [
        NewTopic(
            name="personsSource",
            num_partitions=partitions_persons,
            replication_factor=1
        ),
        NewTopic(
            name="auctionsSource",
            num_partitions=partitions_auctions,
            replication_factor=1
        )
    ]
    try:
        admin_client.create_topics(topics_to_create)
        print("Topics created successfully.")
    except TopicAlreadyExistsError:
        print("Topics already exist.")
    finally:
        admin_client.close()

async def main():
    args = config()

    await ensure_kafka_topics_exist(
        partitions_persons=int(args.persons_partitions),
        partitions_auctions=int(args.auctions_partitions)
    )

    universalis = Universalis(UNIVERSALIS_HOST, UNIVERSALIS_PORT,
                              ingress_type=IngressTypes.KAFKA,
                              kafka_url=KAFKA_URL)
    await universalis.start()

    channel_list = [
        (None, 'personsSource', False),
        (None, 'auctionsSource', False),
        ('personsSource', 'personsFilter', True),
        ('auctionsSource', 'join', True),
        ('personsFilter', 'join', False),
        ('join', 'sink', False),
        ('sink', None, False)
    ]

    await universalis.send_channel_list(channel_list)

    await asyncio.sleep(5)

    ####################################################################################################################
    # SUBMIT STATEFLOW GRAPH ###########################################################################################
    ####################################################################################################################
    scale = int(args.persons_partitions)
    auctions_source_operator.set_partitions(scale)
    persons_source_operator.set_partitions(scale)
    persons_filter_operator.set_partitions(scale)
    join_operator.set_partitions(scale)
    sink_operator.set_partitions(scale)
    q3_graph.g.add_operators(auctions_source_operator, persons_source_operator, persons_filter_operator, join_operator,
                             sink_operator)
    await universalis.submit(q3_graph.g)
    # if(args.bids_partitions + args.interval) == 0:
    #     load_pattern = "static"
    #     load_amplitude = "0"
    # else:
    #     load_pattern = "cosine"
    #     load_amplitude = "300"
    load_pattern = "static" if (args.bids_partitions + args.interval) == 0 else "cosine"
    print('Graph submitted')

    import os
    jar_path = os.path.expanduser("../nexmark/target/nexmark-generator-1.0-SNAPSHOT-jar-with-dependencies.jar")

    # 启动 Nexmark（异步运行）
    nexmark_process = subprocess.Popen(
        ["java", "-jar", jar_path,
        "--query", "3",
        "--generator-parallelism", "1",
        "--enable-auctions-topic", "true",
        "--enable-persons-topic", "true",
        "--load-pattern", load_pattern,
        "--experiment-length", "1",
        "--use-default-configuration", "false",
        "--cosine-period", "10",
        "--input-rate-mean", str(args.rate),
        "--rate", args.rate,
        # "--input-rate-maximum-divergence", load_amplitude,
        "--max-noise", "0",
        "--iteration-duration-ms", "90000",
        "--kafka-server", "localhost:9093",
        "--uni-persons-partitions", args.persons_partitions,
        "--uni-auctions-partitions", args.auctions_partitions,
        "--enable_dynamic_interval", str(args.interval),
        "--enable_dynamic_ratio", str(args.ratio)
        ]
    )

    init_data()

    # 启动监控与预测任务
    asyncio.create_task(monitor_and_predict(args, channel_list))  # 传递 channel_list 参数
    args.rate = 12000

    # 等待 Nexmark 进程结束
    while nexmark_process.poll() is None:
        await asyncio.sleep(1)

    await universalis.close()


uvloop.install()
asyncio.run(main())
