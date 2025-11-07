from aiokafka import AIOKafkaConsumer
import pandas as pd

import asyncio
import uvloop
from universalis.common.serialization import msgpack_deserialization


async def consume():
    import time
    records = []
    consumer = AIOKafkaConsumer(
        'universalis-egress',
        key_deserializer=msgpack_deserialization,
        value_deserializer=msgpack_deserialization,
        bootstrap_servers='localhost:9093',
        auto_offset_reset="earliest",
    )
    await consumer.start()
    try:
        while True:
            data = await consumer.getmany(timeout_ms=1000)
            if not data:
                break
            for _, messages in data.items():
                for msg in messages:
                    records.append((msg.key, *msg.value, msg.timestamp))
    finally:
        await consumer.stop()
        df = pd.DataFrame.from_records(records, columns=['request_id', 'response', 'timestamp'])
        df = df.drop_duplicates(subset=['request_id'], keep='first')
        df.to_csv('kafka_output.csv', index=False)

uvloop.install()
asyncio.run(consume())
