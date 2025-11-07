import asyncio
from universalis.common.operator import BaseOperator
from universalis.common.stateflow_graph import StateflowGraph
from universalis.common.stateflow_worker import StateflowWorker
from universalis.common.logging import logging

from .base_scheduler import BaseScheduler


class RoundRobin(BaseScheduler):

    @staticmethod
    async def schedule(workers: dict[int, tuple[str, int]],
                       execution_graph: StateflowGraph,
                       network_manager):
        operator_partition_locations: dict[str, dict[str, tuple[str, int]]] = {}
        worker_locations = [StateflowWorker(worker[0], worker[1]) for worker in workers.values()]
        worker_assignments: dict[tuple[str, int], list[tuple[BaseOperator, int]]] = {(worker.host, worker.port): []
                                                                                     for worker in worker_locations}

        total_partitions_per_operator = {}

        for operator_name, operator in iter(execution_graph):
            total_partitions_per_operator[operator_name] = operator.n_partitions
            for partition in range(operator.n_partitions):
                current_worker = worker_locations.pop(0)
                worker_assignments[(current_worker.host, current_worker.port)].append((operator, partition))
                if operator_name in operator_partition_locations:
                    operator_partition_locations[operator_name].update({str(partition): (current_worker.host,
                                                                                         current_worker.port)})
                else:
                    operator_partition_locations[operator_name] = {str(partition): (current_worker.host,
                                                                                    current_worker.port)}
                worker_locations.append(current_worker)

        # We want to be able to map a location to a worker id, so we invert the 'workers' dictionary
        location_to_worker_id = {}
        for key in workers.keys():
            location_to_worker_id[workers[key]] = key

        # With this inverted dict, we can map (operator, partition) to a worker id.
        # This can be used in the coordinator service to map the message streams that have to be replayed to the corresponding sender id.
        # For example, if we have to replay filter_1_map_4 from offset x we can look up (filter, 1) to get the id of the worker that should replay from x.
        partitions_to_ids = {}
        for op in operator_partition_locations.keys():
            partitions_to_ids[op] = {}
            for part in operator_partition_locations[op].keys():
                partitions_to_ids[op][part] = location_to_worker_id[operator_partition_locations[op][part]]

        tasks = [
            asyncio.ensure_future(
                network_manager.send_message(worker[0], worker[1],
                                             {"__COM_TYPE__": 'RECEIVE_EXE_PLN',
                                              "__MSG__": (operator_partitions,
                                                          operator_partition_locations,
                                                          workers,
                                                          execution_graph.operator_state_backend,
                                                          total_partitions_per_operator,
                                                          partitions_to_ids)}))
            for worker, operator_partitions in worker_assignments.items()]

        await asyncio.gather(*tasks)

        return partitions_to_ids

        # Should return the operators/partitions per workerid


# import asyncio
# from universalis.common.operator import BaseOperator
# from universalis.common.stateflow_graph import StateflowGraph
# from universalis.common.stateflow_worker import StateflowWorker
# from universalis.common.logging import logging
# from .base_scheduler import BaseScheduler

# class RoundRobin(BaseScheduler):

#     @staticmethod
#     async def schedule(workers: dict[int, tuple[str, int]],
#                        execution_graph: StateflowGraph,
#                        network_manager):
#         operator_partition_locations: dict[str, dict[str, tuple[str, int]]] = {}
#         worker_locations = [StateflowWorker(worker[0], worker[1]) for worker in workers.values()]
#         worker_assignments: dict[tuple[str, int], list[tuple[BaseOperator, int]]] = {(worker.host, worker.port): []
#                                                                                      for worker in worker_locations}

#         total_partitions_per_operator = {}

#         # 遍历每个算子
#         for operator_name, operator in iter(execution_graph):
#             total_partitions_per_operator[operator_name] = operator.n_partitions
#             # 为当前算子选择一个工作节点
#             current_worker = worker_locations.pop(0)
#             # 将当前算子的所有分区分配给同一个工作节点
#             for partition in range(operator.n_partitions):
#                 worker_assignments[(current_worker.host, current_worker.port)].append((operator, partition))
#                 if operator_name in operator_partition_locations:
#                     operator_partition_locations[operator_name].update({str(partition): (current_worker.host,
#                                                                                          current_worker.port)})
#                 else:
#                     operator_partition_locations[operator_name] = {str(partition): (current_worker.host,
#                                                                                     current_worker.port)}
#             # 将工作节点放回列表，以便其他算子可以继续分配
#             worker_locations.append(current_worker)

#         # 创建从分区到工作节点ID的映射
#         location_to_worker_id = {}
#         for key in workers.keys():
#             location_to_worker_id[workers[key]] = key

#         partitions_to_ids = {}
#         for op in operator_partition_locations.keys():
#             partitions_to_ids[op] = {}
#             for part in operator_partition_locations[op].keys():
#                 partitions_to_ids[op][part] = location_to_worker_id[operator_partition_locations[op][part]]

#         # 向工作节点发送分配信息
#         tasks = []
#         for worker, operator_partitions in worker_assignments.items():
#             try:
#                 task = asyncio.ensure_future(
#                     network_manager.send_message(worker[0], worker[1],
#                                                  {"__COM_TYPE__": 'RECEIVE_EXE_PLN',
#                                                   "__MSG__": (operator_partitions,
#                                                               operator_partition_locations,
#                                                               workers,
#                                                               execution_graph.operator_state_backend,
#                                                               total_partitions_per_operator,
#                                                               partitions_to_ids)}))
#                 tasks.append(task)
#             except Exception as e:
#                 logging.error(f"Failed to send message to worker {worker}: {e}")

#         await asyncio.gather(*tasks)

#         return partitions_to_ids