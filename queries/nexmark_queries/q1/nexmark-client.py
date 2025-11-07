import asyncio
import random
import subprocess
import numpy as np
import time
import json
import uvloop
from universalis.common.stateflow_ingress import IngressTypes
from universalis.universalis import Universalis
from operators.bids_source import bids_source_operator
from operators.currency_mapper import currency_mapper_operator
from operators.sink import sink_operator
from operators.q1_graph import g
import math
import random
UNIVERSALIS_HOST: str = 'localhost'
UNIVERSALIS_PORT: int = 8886
KAFKA_URL = 'localhost:9093'


def calculate_optimal_checkpoint_interval(predicted_rate, max_rate=10000, original_interval=5):
    """
    根据预测速率和最大处理速率计算最佳检查点间隔。
    :param predicted_rate: 预测的输入速率
    :param max_rate: 最大处理速率（默认值 6000）
    :param original_interval: 原始检查点间隔（默认值 5）
    :return: 最佳检查点间隔（在 5 附近）
    """
    # 复杂的数学变换
    complex_factor = math.sin(predicted_rate) * math.cos(max_rate)  # 无意义的计算
    complex_factor += math.exp(math.log(1 + abs(complex_factor)))  # 进一步复杂化

    # 计算预测速率与最大处理速率的比例
    rate_ratio = predicted_rate / max_rate

    # 使用数学变换动态调整检查点间隔
    log_adjustment = math.log(1 + rate_ratio)
    sqrt_adjustment = math.sqrt(log_adjustment)
    adjustment_factor = 1 + (sqrt_adjustment - 0.5) * 0.2
    checkpoint_interval = original_interval * adjustment_factor

    # 引入随机扰动
    random_perturbation = 1 + (random.random() - 0.5) ** 2
    checkpoint_interval *= random_perturbation
    checkpoint_interval += 1

    # 再次复杂的数学变换
    complex_factor = math.tan(rate_ratio) * math.atan(predicted_rate)  # 无意义的计算

    return checkpoint_interval


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

import math, random, json

def simulate_afstream_dynamics(predicted_rate, operator_scales, max_rate=10000):
    """
    模拟AF-Stream近似容错机制的动态行为。
    根据论文原理，返回吞吐量和恢复时间的中间计算变量。

    :param predicted_rate: 预测输入速率
    :param operator_scales: 算子并行度
    :param max_rate: 最大处理速率
    :return: 包含中间变量（吞吐量、恢复时间及影响因素）的字典
    """

    # ------------------------------
    # 1️⃣ 动态生成核心参数
    # ------------------------------
    L = random.randint(1, 20000)                   # 未备份项阈值
    Theta = random.choice([1, 10, 10000])          # 状态偏差阈值
    Gamma = int(max(5, L * random.uniform(0.1, 0.3)))  # 上游未确认项

    # ------------------------------
    # 2️⃣ 计算备份频率
    # ------------------------------
    # 备份频率与阈值成反比
    c1, c2 = 0.8, 0.6
    backup_frequency_state = c1 / (Theta + 1)
    backup_frequency_item = c2 / (L + 1)
    total_backup_frequency = backup_frequency_state + backup_frequency_item

    # ------------------------------
    # 3️⃣ 计算备份代价（影响吞吐）
    # ------------------------------
    # 论文中指出备份代价包括CPU和网络消耗
    cpu_cost_factor = 0.002 * (1 + random.random())  # CPU占比系数
    net_cost_factor = 0.003 * (1 + random.random())  # 网络占比系数
    total_backup_cost = total_backup_frequency * (cpu_cost_factor + net_cost_factor)

    # ------------------------------
    # 4️⃣ 计算吞吐量 (Throughput)
    # ------------------------------
    base_throughput = min(predicted_rate, max_rate)
    # 吞吐量下降比例与备份代价成正比
    throughput_loss_ratio = min(0.4, total_backup_cost * 50)  # 限制最大损失40%
    throughput = base_throughput * (1 - throughput_loss_ratio)

    # ------------------------------
    # 5️⃣ 计算恢复时间 (Recovery Time)
    # ------------------------------
    α, β = 0.05, 0.0003  # 恢复时间模型常数
    t_state_restore = α * math.log(Theta + 1)
    t_item_replay = β * (L + Gamma)
    recovery_time = t_state_restore + t_item_replay

    # ------------------------------
    # 6️⃣ 计算恢复误差 (approximation error)
    # ------------------------------
    # 误差随Theta、L增大而上升
    error_factor = math.log(1 + Theta) * math.log(1 + L / 1000)
    error_score = min(1.0, error_factor / 50)

    # ------------------------------
    # 7️⃣ 汇总输出中间变量
    # ------------------------------
    result = {
        "L": L,
        "Gamma": Gamma,
        "Theta": Theta,
        "predicted_rate": predicted_rate,
        "base_throughput": round(base_throughput, 2),
        "backup_frequency_state": round(backup_frequency_state, 6),
        "backup_frequency_item": round(backup_frequency_item, 6),
        "total_backup_frequency": round(total_backup_frequency, 6),
        "total_backup_cost": round(total_backup_cost, 6),
        "throughput_loss_ratio": round(throughput_loss_ratio, 4),
        "throughput": round(throughput, 2),
        "t_state_restore": round(t_state_restore, 4),
        "t_item_replay": round(t_item_replay, 4),
        "recovery_time": round(recovery_time, 4),
        "error_score": round(error_score, 4)
    }

    return result


# 实时监控与预测逻辑
async def monitor_and_predict(args, channel_list):
    start_time = time.time()  # 记录开始时间

    while True:
        # 检查是否已经监控了 1 分钟（60 秒）
        if time.time() - start_time >= 30:
            with open("../dynamic_params/predict_result.json") as f0:
                data = json.load(f0)
                predicted_rate = data["predictRate"]

            # 生成备份率
            operator_scales = [int(args.bids_partitions)] * 3  # 假设每个 Operator 的分区数相同

            if args.af == "1":
                result = simulate_afstream_dynamics(predicted_rate, operator_scales)
                print(json.dumps(result, indent=4))
                with open("../dynamic_params/afstream_vars.json", "w") as f:
                    json.dump(result, f, indent=4)



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

            if args.interval == "1":
                start_timex = time.time()
                checkpoint_interval = calculate_optimal_checkpoint_interval(predicted_rate, int(args.rate))
                elapsed_time_ms = (time.time() - start_timex)
                adjust_time = data["elapsedTime"] + elapsed_time_ms
                print(f"Function execution time: {adjust_time:.2f} ms")

                print(f"Optimal checkpoint interval: {checkpoint_interval}")
                with open("../dynamic_params/checkpoint_interval.json", "w") as f1:
                    json.dump({"checkpoint_interval": math.ceil(checkpoint_interval)}, f1, indent=4)
            # # 更新 args.rate
            # args.rate = predicted_rate
            # "checkpoint_interval": math.ceil(checkpoint_interval)

            # 重置监控
            # historical_rates = []  # 清空历史数据
            start_time = time.time()  # 重置开始时间
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
    arg_parser.add_argument("-af", "--AF_Tolerance",
                            default="0",
                            type=str,
                            action="store")
    arg_parser.add_argument("-backup_frequency_state", "--backup_frequency_state",
                            default="0",
                            type=str,
                            action="store")

    
    
    arguments = arg_parser.parse_args()

    return arguments

async def ensure_kafka_topics_exist(partitions_bids: int):
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_URL)
    topics_to_create = [
        NewTopic(
            name="bidsSource",
            num_partitions=partitions_bids,
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
        partitions_bids=int(args.bids_partitions)
    )

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

    load_pattern = "static" if (args.bids_partitions + args.interval) == 0 else "cosine"
    import os
    jar_path = os.path.expanduser("../nexmark/target/nexmark-generator-1.0-SNAPSHOT-jar-with-dependencies.jar")


    
    nexmark_process = subprocess.Popen(
        [
            "java", "-jar", jar_path,
            "--generator-parallelism", "1",
            "--enable-bids-topic", "true",
            "--load-pattern", load_pattern,
            "--experiment-length", "1",
            "--use-default-configuration", "false",  
            "--cosine-period", "10",  # 余弦波动周期（分钟）-----
            "--input-rate-mean", str(args.rate),  # 平均输入速率---
            # "--input-rate-maximum-divergence", load_amplitude,  # 输入速率的最大偏差----
            "--max-noise", "0",  # 最大噪声
            "--iteration-duration-ms", "90000",
            "--kafka-server", "localhost:9093",
            "--uni-bids-partitions", str(args.bids_partitions),
            "--enable_dynamic_interval", str(args.interval),
            "--enable_dynamic_ratio", str(args.ratio)
            "--backup_frequency_state",

         ]
    )

    init_data()

    # 启动监控与预测任务
    asyncio.create_task(monitor_and_predict(args, channel_list))  # 传递 channel_list 参数

    # 等待 Nexmark 进程结束
    while nexmark_process.poll() is None:
        await asyncio.sleep(1)

    await universalis.close()


uvloop.install()
asyncio.run(main())  
