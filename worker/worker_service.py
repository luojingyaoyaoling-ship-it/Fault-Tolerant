import asyncio
import io
import os
import sys
import time
import random
from math import ceil
import concurrent.futures
from timeit import default_timer as timer

import aiozmq
import uvloop
import zmq
from aiokafka import AIOKafkaConsumer, TopicPartition

from minio import Minio

from universalis.common.local_state_backends import LocalStateBackend
from universalis.common.logging import logging
from universalis.common.networking import NetworkingManager
from universalis.common.operator import Operator
from universalis.common.serialization import (Serializer, pickle_deserialization,
                                              pickle_serialization, msgpack_serialization)
from universalis.common.kafka_producer_pool import KafkaProducerPool
from universalis.common.kafka_consumer_pool import KafkaConsumerPool

from worker.config import config
from worker.operator_state.in_memory_state import InMemoryOperatorState
from worker.operator_state.stateless import Stateless
from worker.run_func_payload import RunFuncPayload
from worker.checkpointing.uncoordinated_checkpointing import UncoordinatedCheckpointing

SERVER_PORT: int = 8888 # coordianator 的端口
DISCOVERY_HOST: str = os.environ['DISCOVERY_HOST']
DISCOVERY_PORT: int = int(os.environ['DISCOVERY_PORT'])
KAFKA_URL: str = os.getenv('KAFKA_URL', None)
INGRESS_TYPE = os.getenv('INGRESS_TYPE', None)
EGRESS_TOPIC_NAME: str = 'universalis-egress'

MINIO_URL: str = f"{os.environ['MINIO_HOST']}:{os.environ['MINIO_PORT']}"
MINIO_ACCESS_KEY: str = os.environ['MINIO_ROOT_USER']
MINIO_SECRET_KEY: str = os.environ['MINIO_ROOT_PASSWORD']
SNAPSHOT_BUCKET_NAME: str = "universalis-snapshots"


class Worker(object):

    def __init__(self, failure: bool = False, cascade_failure: bool = False):
        self.checkpoint_protocol = None
        self.checkpoint_interval = 5
        self.checkpointing = None
        self.failure: bool = failure
        self.cascade_failure = cascade_failure
        self.channel_list = None

        self.id: int = -1
        self.networking = NetworkingManager()
        self.router = None
        self.kafka_egress_producer_pool = None
        self.kafka_ingress_consumer_pool = None
        self.operator_state_backend = None
        self.registered_operators: dict[tuple[str, int], Operator] = {}
        self.dns: dict[str, dict[str, tuple[str, int]]] = {}
        self.topic_partitions: dict[tuple[str, int], tuple[TopicPartition, int]] = {}
        self.peers: dict[int, tuple[str, int]] = {}
        self.local_state: InMemoryOperatorState | Stateless = Stateless()
        self.background_tasks = set()
        self.total_partitions_per_operator = {}
        self.function_tasks = set()
        self.minio_client: Minio = Minio(
            MINIO_URL, access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY, secure=False
        )
        self.snapshot_event: asyncio.Event = asyncio.Event()
        self.snapshot_event.set()
        self.waiting_for_exe_graph = True
        self.can_start_checkpointing = False

        self.offsets_per_second = {}

        self.snapshot_taken_message_size = 0

        self.puller_task = ...

        self.consumer_tasks: list[asyncio.Task] = []
        self.performing_recovery: bool = False
        self.unc_log_replay_tasks: list[asyncio.Task] = []
        self.running_replayed: asyncio.Event = asyncio.Event()
        self.running_replayed.set()
        self.cor_channel_block_on_marker: dict[str, dict[str, asyncio.Event]] = {}

        self.backpressure_queue = asyncio.Queue(maxsize=1000)
        self.buffer_runner_task: asyncio.Task = ...
        self.max_concurrency = 1000
        self.concurrency_condition: asyncio.Condition = asyncio.Condition()

        self.AF_Tolerance = 0
        self.af_throughput = 0
        self.recovery_time = 0

    async def run_function(
            self,
            payload: RunFuncPayload,
            send_from=None,
            replayed=False
    ) -> bool:

        if (self.performing_recovery or payload.recovery_cycle != self.networking.recovery_cycle) and not replayed:
            return False

        success: bool = True
        operator_partition = self.registered_operators[(payload.operator_name, payload.partition)]
        response = await operator_partition.run_function(
            payload.key,
            payload.request_id,
            payload.timestamp,
            payload.function_name,
            payload.params,
            payload.partition
        )

        if send_from is not None and self.checkpoint_protocol == "UNC":
            tmp_str = (send_from['operator_partition'] * self.total_partitions_per_operator[payload.operator_name]
                       + payload.partition)
            incoming_channel = f"{send_from['operator_name']}_{payload.operator_name}_{tmp_str}"
            self.checkpointing.set_last_messages_processed(payload.operator_name,
                                                           incoming_channel,
                                                           send_from['kafka_offset'])

        if isinstance(response, Exception):
            success = False
        if payload.response_socket is not None:
            self.router.write(
                (payload.response_socket, self.networking.encode_message(
                    response,
                    Serializer.MSGPACK
                ))
            )
        elif response is not None:
            if isinstance(response, Exception):
                kafka_response = str(response)
            else:
                kafka_response = response
            self.create_task(next(self.kafka_egress_producer_pool).send_and_wait(
                EGRESS_TOPIC_NAME,
                key=payload.request_id,
                value=msgpack_serialization(kafka_response),
                partition=self.id - 1
            ))

        return success

    @staticmethod
    def async_snapshot(
            snapshot_name,
            snapshot_data,
            message_encoder,
            coordinator_info: dict | None = None):
        minio_client: Minio = Minio(
            MINIO_URL, access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY, secure=False
        )
        bytes_file: bytes = pickle_serialization(snapshot_data)
        minio_client.put_object(bucket_name=SNAPSHOT_BUCKET_NAME,
                                object_name=snapshot_name,
                                data=io.BytesIO(bytes_file),
                                length=len(bytes_file))

        if coordinator_info is not None:
            msg = message_encoder(
                {
                    "__COM_TYPE__": 'SNAPSHOT_TAKEN',
                    "__MSG__": coordinator_info
                },
                Serializer.MSGPACK)
            sync_socket_to_coordinator = zmq.Context().socket(zmq.PUSH)
            sync_socket_to_coordinator.connect(f'tcp://{DISCOVERY_HOST}:{DISCOVERY_PORT}')
            sync_socket_to_coordinator.send(msg)
            sync_socket_to_coordinator.close()
            return msgpack_serialization({
                "__COM_TYPE__": 'SNAPSHOT_TAKEN',
                "__MSG__": coordinator_info
            }).__sizeof__()
        return 0

    async def take_snapshot(self,
                            pool: concurrent.futures.ProcessPoolExecutor,
                            operator,
                            cic_clock=0,
                            cor_round=-1,
                            sn_time=-1):
        if isinstance(self.local_state, InMemoryOperatorState):
            snapshot_start = time.time_ns() // 1000000
            self.local_state: InMemoryOperatorState
            if self.function_tasks:
                await asyncio.gather(*self.function_tasks)
            last_messages_sent = self.networking.return_last_offset(operator)
            snapshot_data = {}
            af_result = {}
            if self.checkpoint_protocol == 'UNC':
                snapshot_data = self.checkpointing.get_snapshot_data(operator, last_messages_sent)
            elif self.AF_Tolerance == 'true':
                af_result = self.checkpointing.recalc_throughput_recovery(operator, last_messages_sent)
            else:
                logging.warning('Unknown protocol, no snapshot data added.')
            snapshot_data['local_state_data'] = self.local_state.data[operator]
            af_result['throughput'] = self.af_throughput
            af_result['recovery_time'] = self.recovery_time
            snapshot_time = cor_round
            if snapshot_time == -1:
                snapshot_time = sn_time
            snapshot_name: str = f"snapshot_{self.id}_{operator}_{snapshot_time}.bin"
            coordinator_info = None
            if ((self.checkpoint_protocol == 'UNC' or self.checkpoint_protocol == 'CIC') and not self.performing_recovery):
                snapshot_duration = (time.time_ns() // 1000000) - snapshot_start
                coordinator_info = {'last_messages_processed': snapshot_data['last_messages_processed'],
                                    'last_messages_sent': last_messages_sent,
                                    'snapshot_name': snapshot_name,
                                    'snapshot_duration': snapshot_duration}

            loop = asyncio.get_running_loop()
            loop.run_in_executor(pool,
                                 self.async_snapshot,
                                 snapshot_name,
                                 snapshot_data,
                                 self.networking.encode_message,
                                 coordinator_info)  # .add_done_callback(self.update_network_sizes)
            snapshot_end = time.time_ns() // 1000000
            if snapshot_end - snapshot_start > 1000:
                logging.warning(f'Operator: {operator}, Total time: {snapshot_end - snapshot_start}')
        else:
            logging.warning("Snapshot currently supported only for in-memory operator state")

    def update_network_sizes(self, future):
        self.networking.total_network_size += future.result()
        if self.checkpoint_protocol == 'UNC':
            self.networking.additional_uncoordinated_size += future.result()

    def restore_from_snapshot(self, snapshot_to_restore, operator_name):
        state_to_restore = self.minio_client.get_object(
            bucket_name=SNAPSHOT_BUCKET_NAME,
            object_name=snapshot_to_restore
        ).data
        deserialized_data = pickle_deserialization(state_to_restore)
        self.local_state.data[operator_name] = deserialized_data['local_state_data']
        last_kafka_consumed = {}
        if 'last_kafka_consumed' in deserialized_data.keys():
            last_kafka_consumed = deserialized_data['last_kafka_consumed']
        to_replay = self.checkpointing.restore_snapshot_data(operator_name,
                                                                 deserialized_data['last_messages_processed'],
                                                                 deserialized_data['last_messages_sent'],
                                                                 last_kafka_consumed)
        logging.warning(f"operator_name: {operator_name}"
                        f" last_messages_processed: {deserialized_data['last_messages_processed']}"
                        f" last_messages_sent: {deserialized_data['last_messages_sent']}"
                        f" last_kafka_consumed: {last_kafka_consumed}")
        return to_replay

    async def start_kafka_consumer(self, topic_partitions: list[tuple[TopicPartition, int]]):
        logging.warning(f'Creating Kafka consumer per topic partitions: {topic_partitions}')
        self.kafka_ingress_consumer_pool = KafkaConsumerPool(self.id, KAFKA_URL, topic_partitions)
        await self.kafka_ingress_consumer_pool.start()
        for consumer in self.kafka_ingress_consumer_pool.consumer_pool.values():
            logging.warning(f'Creating Kafka consumer: {consumer}')
            self.consumer_tasks.append(asyncio.create_task(self.consumer_read(consumer)))

    async def stop_kafka_consumer(self):
        while self.consumer_tasks:
            consumer_task = self.consumer_tasks.pop()
            consumer_task.cancel()
            try:
                await consumer_task
            except asyncio.CancelledError:
                pass
        await self.kafka_ingress_consumer_pool.close()
        self.kafka_ingress_consumer_pool = None
        logging.warning("Kafka Ingress Stopped")

    async def replay_from_log(self, operator_name, log_file_name, offset):
        replay_until = self.checkpointing.find_last_sent_offset(operator_name, log_file_name)

        if replay_until is not None and replay_until > offset:
            self.networking.log_seek(log_file_name, offset)
            count = 0
            offset += 1
            message_offset = offset + count
            while replay_until > message_offset:
                message_offset = offset + count
                count += 1
                message = self.networking.get_log_line(log_file_name, message_offset)
                await self.replay_log_message(message, message_offset)
            logging.warning(f"replayed {count} messages")

    async def replay_log_message(self, deserialized_data, message_offset):
        receiver_info = deserialized_data['__MSG__']['__SENT_TO__']
        deserialized_data['__MSG__']['__SENT_FROM__']['kafka_offset'] = message_offset
        deserialized_data['__MSG__']['__REPLAYED__'] = True
        await self.networking.replay_message(receiver_info['host'], receiver_info['port'], deserialized_data)

    async def clear_function_tasks(self):
        while self.function_tasks:
            t = self.function_tasks.pop()
            t.cancel()
            try:
                await t
                async with self.concurrency_condition:
                    self.concurrency_condition.notify_all()
            except asyncio.CancelledError:
                pass
        logging.warning("All function are canceled.")

    async def simple_failure(self):
        sleep_time = 48 - int(int(self.id) - 1) * (48 // (len(self.peers) + 1))
        # match int(self.id):
        #     case 1:
        #         sleep_time =50
        #     case 2:
        #         return
        #         # sleep_time = 40
        #     case 3:
        #         return 
        #         # sleep_time = 30
        #     case 4:
        #         return
        #         # sleep_time = 20
        #     case 5:
        #         sleep_time = 10
        #     case _:
        #         sleep_time = 50
        # if int(self.id) == 5:
        #     sleep_time = 50
        # elif int(self.id) == 1:
        #     sleep_time = 40
        await asyncio.sleep(sleep_time)
        logging.warning(f"-----------------{sleep_time}---{int(self.id)}--------------")
        logging.error("----------------MORS PRINCIPIUM EST----------------")
        await self.networking.send_message(
            DISCOVERY_HOST, DISCOVERY_PORT,
            {
                "__COM_TYPE__": 'WORKER_FAILED',
                "__MSG__": self.id
            },
            Serializer.MSGPACK
        )

    async def consumer_read(self, consumer: AIOKafkaConsumer):
        while True:
            result = await consumer.getmany(timeout_ms=1)
            for _, messages in result.items():
                for message in messages:
                    if len(self.function_tasks) < self.max_concurrency:
                        await self.handle_message_from_kafka(message)
                    else:
                        async with self.concurrency_condition:
                            await self.concurrency_condition.wait_for(
                                lambda: len(self.function_tasks) < self.max_concurrency)
                            await self.handle_message_from_kafka(message)

    async def buffer_consumer(self):
        while True:
            message = await self.backpressure_queue.get()
            await self.handle_message_from_kafka(message)

    async def handle_message_from_kafka(self, msg):
        logging.info(
            f"Consumed: {msg.topic} {msg.partition} {msg.offset} "
            f"{msg.key} {msg.value} {msg.timestamp}"
        )

        deserialized_data: dict = self.networking.decode_message(msg.value)
        message_type: str = deserialized_data['__COM_TYPE__']
        message = deserialized_data['__MSG__']
        if message_type == 'RUN_FUN':
            run_func_payload: RunFuncPayload = self.unpack_run_payload(message,
                                                                       msg.key,
                                                                       timestamp=msg.timestamp,
                                                                       rec_cycle=self.networking.recovery_cycle)
            logging.info(f'RUNNING FUNCTION FROM KAFKA: {run_func_payload.function_name} {run_func_payload.key}')
            if not self.can_start_checkpointing:
                self.can_start_checkpointing = True
                self.create_task(self.notify_coordinator())

                if self.failure:
                    logging.warning("2222")
                    if self.cascade_failure:
                        logging.warning("3333")
                        self.create_task(self.simple_failure())
                    else:
                        if self.id == 1:
                            self.create_task(self.simple_failure())
                

            await self.snapshot_event.wait()
            if self.checkpointing is not None:
                self.checkpointing.set_consumed_offset(msg.topic, msg.partition, msg.offset)

            if message['__FUN_NAME__'] == 'trigger':
                self.create_task(
                    self.run_function(
                        run_func_payload
                    )
                )
            else:
                self.create_run_function_task(
                    self.run_function(
                        run_func_payload
                    )
                )
        else:
            logging.error(f"Invalid message type: {message_type} passed to KAFKA")

    async def notify_coordinator(self):
        await self.networking.send_message(
            DISCOVERY_HOST, DISCOVERY_PORT,
            {
                "__COM_TYPE__": 'STARTED_PROCESSING',
                "__MSG__": self.id
            },
            Serializer.MSGPACK
        )

    def create_task(self, coroutine):
        task = asyncio.create_task(coroutine)
        self.background_tasks.add(task)
        task.add_done_callback(self.background_tasks.discard)

    def create_run_function_task(self, coroutine):
        task = asyncio.create_task(coroutine)
        self.function_tasks.add(task)
        asyncio.create_task(self.discard_and_notify(task))

    async def discard_and_notify(self, task: asyncio.Task):
        await task
        self.function_tasks.discard(task)
        async with self.concurrency_condition:
            self.concurrency_condition.notify_all()

    async def checkpoint_coordinated_sources(self, pool, sources, _round):
        for source in sources:
            outgoing_channels = self.checkpointing.get_outgoing_channels(source)
            await self.take_snapshot(pool, source, cor_round=_round)
            await asyncio.gather(*[self.send_marker(source, _id, operator, _round)
                                   for _id, operator in outgoing_channels])

    async def send_marker(self, source, _id, operator, _round):
        if _id not in self.peers.keys():
            logging.warning('Unknown id in network')
        else:
            (host, port) = self.peers[_id]
            await self.networking.send_message(
                host, port,
                {
                    "__COM_TYPE__": 'COORDINATED_MARKER',
                    "__MSG__": (self.id, source, operator, _round)
                },
                Serializer.MSGPACK
            )

    async def worker_controller(self, pool: concurrent.futures.ProcessPoolExecutor, deserialized_data, resp_adr):
        message_type: str = deserialized_data['__COM_TYPE__']
        message = deserialized_data['__MSG__']
        match message_type:
            case 'RUN_FUN_REMOTE' | 'RUN_FUN_RQ_RS_REMOTE':
                request_id = message['__RQ_ID__']
                operator_name = message['__OP_NAME__']
                if message_type == 'RUN_FUN_REMOTE':
                    if self.checkpoint_protocol == 'UNC':
                        await self.running_replayed.wait()

                    sender_details = message['__SENT_FROM__']
                    payload = self.unpack_run_payload(message, request_id)
                    replayed: bool = True if '__REPLAYED__' in message else False
                    self.create_run_function_task(
                        self.run_function(
                            payload, send_from=sender_details, replayed=replayed
                        )
                    )
                else:
                    logging.info('CALLED RUN FUN RQ RS FROM PEER')
                    payload = self.unpack_run_payload(message, request_id, response_socket=resp_adr)
                    await self.snapshot_event.wait()
                    self.create_run_function_task(
                        self.run_function(
                            payload
                        )
                    )
            case 'CHECKPOINT_PROTOCOL':
                self.checkpoint_protocol = message[0]
                self.checkpoint_interval = message[1]
                self.networking.set_checkpoint_protocol(message[0])
            case 'SEND_CHANNEL_LIST':
                self.channel_list = message
            case 'GET_METRICS':
                logging.warning('METRIC REQUEST RECEIVED.')
                return_value = (self.offsets_per_second,
                                self.networking.get_total_network_size(),
                                self.networking.get_protocol_network_size())
                reply = self.networking.encode_message(return_value, Serializer.MSGPACK)
                self.router.write((resp_adr, reply))
            case 'RECOVER_FROM_SNAPSHOT':
                logging.warning('Recovery message received.')
                self.performing_recovery = True
                self.networking.increment_recovery_cycle()
                if self.checkpoint_protocol in ['UNC', 'CIC']:
                    self.running_replayed.clear()

                await self.stop_kafka_consumer()
                await self.clear_function_tasks()
                await self.networking.close_all_connections()
                if self.checkpoint_protocol in ['UNC', 'CIC']:
                    self.networking.close_logfiles()
                    self.networking.open_logfiles_for_read()
                self.unc_log_replay_tasks = []
                if self.checkpoint_protocol == "UNC":
                    operator_name: str
                    for operator_name in message:
                        logging.warning(f"Recovering operator: {operator_name}")
                        snapshot_timestamp = message[operator_name][0]
                        if snapshot_timestamp == 0:
                            self.local_state.data[operator_name] = {}
                            tp_to_reset = self.checkpointing.reset_messages_processed(operator_name)
                            tp: TopicPartition
                            for tp in tp_to_reset:
                                self.topic_partitions[(operator_name, tp.partition)] = (tp, 0)
                        else:
                            snapshot_to_restore = f'snapshot_{self.id}_{operator_name}_{snapshot_timestamp}.bin'
                            to_replay = self.restore_from_snapshot(snapshot_to_restore, operator_name)
                            for tp, offset in to_replay:
                                self.topic_partitions[(operator_name, tp.partition)] = (tp, offset)
                        for channel in message[operator_name][1].keys():
                            logging.warning(f"started_replaying {channel}")
                            replay_from_log_task = self.replay_from_log(operator_name,
                                                                        channel,
                                                                        message[operator_name][1][channel])
                            self.unc_log_replay_tasks.append(replay_from_log_task)
                else:
                    logging.error('Snapshot restore message received for unknown protocol, no restoration.')
                    return
                logging.warning('REC GLOBAL_RECOVERY_DONE')
                await self.networking.send_message(
                    DISCOVERY_HOST, DISCOVERY_PORT,
                    {
                        "__COM_TYPE__": 'RECOVERY_DONE',
                        "__MSG__": self.id
                    },
                    Serializer.MSGPACK
                )
            case 'GLOBAL_RECOVERY_DONE':
                logging.warning('REC GLOBAL_RECOVERY_DONE')
                if self.checkpoint_protocol in ['UNC', 'CIC']:
                    await asyncio.gather(*self.unc_log_replay_tasks)
                    self.unc_log_replay_tasks = []
                    self.networking.close_logfiles()
                    self.networking.open_logfiles_for_append()
                    self.running_replayed.set()
                self.performing_recovery = False
                await self.start_kafka_consumer(list(self.topic_partitions.values()))
            case 'RECEIVE_EXE_PLN':
                await self.handle_execution_plan(pool, message)
                self.attach_state_to_operators()
            case _:
                logging.error(f"Worker Service: Non supported command message type: {message_type}")

    def attach_state_to_operators(self):
        operator_names: set[str] = set([operator.name for operator in self.registered_operators.values()])
        if self.operator_state_backend == LocalStateBackend.DICT:
            self.local_state = InMemoryOperatorState(operator_names)
        else:
            logging.error(f"Invalid operator state backend type: {self.operator_state_backend}")
            return
        for operator in self.registered_operators.values():
            operator.attach_state_networking(self.local_state, self.networking, self.dns)

    async def send_throughputs(self):
        while True:
            await asyncio.sleep(1)
            offsets = self.checkpointing.get_offsets()
            for operator in offsets.keys():
                if operator not in self.offsets_per_second.keys():
                    self.offsets_per_second[operator] = {}
                for part in offsets[operator].keys():
                    if part not in self.offsets_per_second[operator].keys():
                        self.offsets_per_second[operator][part] = []
                    self.offsets_per_second[operator][part].append(offsets[operator][part])

    async def handle_execution_plan(self, pool, message):
        (worker_operators, self.dns, self.peers, self.operator_state_backend,
         self.total_partitions_per_operator, partitions_to_ids) = message
        logging.warning(f"{self.dns}\n {self.total_partitions_per_operator}\n{self.networking.host_name}")
            

        self.networking.set_id(self.id)
        self.waiting_for_exe_graph = False
        if self.checkpoint_protocol == "UNC":
            self.checkpointing = UncoordinatedCheckpointing()
            self.checkpointing.set_id(self.id)
            self.checkpointing.init_attributes_per_operator(self.total_partitions_per_operator.keys())
        else:
            logging.warning('Not supported value is set for CHECKPOINTING_PROTOCOL, continue without checkpoints.')
        self.networking.set_checkpointing(self.checkpointing)
        if self.checkpoint_protocol == "UNC":
            del self.peers[self.id]
            self.create_task(self.uncoordinated_checkpointing(pool, self.checkpoint_interval))
            logging.warning("uncoordinated checkpointing started")
        else:
            logging.warning("no check protrocol ggg")
            logging.info('no checkpointing started.')
        await self.networking.set_total_partitions_per_operator(self.total_partitions_per_operator)
        operator: Operator
        for tup in worker_operators:
            operator, partition = tup
            self.registered_operators[(operator.name, partition)] = operator
            if INGRESS_TYPE == 'KAFKA':
                self.topic_partitions[(operator.name, partition)] = (TopicPartition(operator.name, partition), 0)
        self.networking.init_log_files(self.channel_list, list(self.registered_operators.keys()))
        logging.warning("start_kafka_cosnsumer")
        await self.start_kafka_consumer(list(self.topic_partitions.values()))
        logging.warning("start_kafka_consumer")


    async def uncoordinated_checkpointing(self, pool, checkpoint_interval):
        import json
        while True:
            with open("/usr/local/universalis/dynamic_params/checkpoint_interval.json") as f:
                data = json.load(f)
            checkpoint_interval = float(data["checkpoint_interval"])
            interval_randomness = random.randint(checkpoint_interval - 0, checkpoint_interval + 1)
            await asyncio.sleep(interval_randomness)
            if not self.performing_recovery and self.can_start_checkpointing:
                self.snapshot_event.clear()
                sn_time = time.time_ns() // 1_000_000
                await asyncio.gather(*[self.take_snapshot(pool, operator, sn_time=sn_time)
                                       for operator in self.total_partitions_per_operator])
                self.snapshot_event.set()

    async def start_tcp_service(self):
        with concurrent.futures.ProcessPoolExecutor(1) as pool:
            self.puller_task = asyncio.create_task(self.start_puller(pool))
            self.router = await aiozmq.create_zmq_stream(zmq.ROUTER, bind=f"tcp://0.0.0.0:{int(SERVER_PORT) + 1}",
                                                         high_read=0, high_write=0)
            self.kafka_egress_producer_pool = KafkaProducerPool(self.id, KAFKA_URL, size=100)
            await self.kafka_egress_producer_pool.start()

            logging.warning(
                f"Worker TCP Server listening at 0.0.0.0:{SERVER_PORT} "
                f"IP:{self.networking.host_name}"
            )
            try:
                while True:
                    resp_adr, data = await self.router.read()
                    deserialized_data: dict = self.networking.decode_message(data)
                    if '__COM_TYPE__' not in deserialized_data:
                        logging.error("Deserialized data do not contain a message type")
                    else:
                        self.create_task(self.worker_controller(pool, deserialized_data, resp_adr))
            finally:
                await self.kafka_egress_producer_pool.close()

    async def start_puller(self, pool):
        puller = await aiozmq.create_zmq_stream(zmq.PULL, bind=f"tcp://0.0.0.0:{SERVER_PORT}",
                                                high_read=0, high_write=0)
        while True:
            data = await puller.read()
            deserialized_data: dict = self.networking.decode_message(data[0])
            if '__COM_TYPE__' not in deserialized_data:
                logging.error("Deserialized data do not contain a message type")
            else:
                self.create_task(self.worker_controller(pool, deserialized_data, None))

    @staticmethod
    def unpack_run_payload(
            message: dict, request_id: bytes,
            timestamp=None, response_socket=None, rec_cycle=-1
    ) -> RunFuncPayload:
        timestamp = message['__TIMESTAMP__'] if timestamp is None else timestamp
        rec_cyc = int(message['__REC_CYC__']) if '__REC_CYC__' in message else rec_cycle
        return RunFuncPayload(
            request_id, message['__KEY__'], timestamp,
            message['__OP_NAME__'], message['__PARTITION__'],
            message['__FUN_NAME__'], tuple(message['__PARAMS__']), response_socket, rec_cyc
        )

    async def register_to_coordinator(self):  
        self.id = await self.networking.send_message_request_response(
            DISCOVERY_HOST, DISCOVERY_PORT,
            {
                "__COM_TYPE__": 'REGISTER_WORKER', 
                "__MSG__": self.networking.host_name
            },
            Serializer.MSGPACK
        )

        logging.warning(f"Worker id: {self.id} G")

    async def main(self):
        await self.register_to_coordinator()
        await self.start_tcp_service()
        self.kafka_ingress_consumer_pool.close()


if __name__ == "__main__":
    args = config()
    worker = Worker(args.failure, args.cascade_failure)
    uvloop.run(worker.main())
