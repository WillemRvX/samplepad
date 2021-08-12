#!/usr/bin/env python

import json
import os
import yaml
from collections import defaultdict
import psycopg2
from confluent_kafka import Consumer, KafkaException, KafkaError


def configs() -> dict[str, str]:
    with open('/sub/configs/specs.yaml') as confs:
        return yaml \
            .safe_load(
                confs
            )


def etl_on_the_fly() -> None:

    meta = configs()
    con_conf = {
        'bootstrap.servers': meta['boostrap_servers'],
        'group.id': meta['group_id'],
        'enable.auto.commit': False,
        'auto.offset.reset': meta['auto_offset_reset'],
    }

    def con(ETL: callable) -> None:
        data, consumer = list(), Consumer(con_conf)
        try:
            consumer.subscribe([meta['topic'], ])
            while True:
                mssg = consumer.poll(timeout=1.0)
                if mssg is None: continue
                if mssg.error():
                    if mssg.error().code() in {KafkaError._PARTITION_EOF, }:
                        err = f'{mssg.topic()} EOF reached at {mssg.offset()}'
                        print(err)
                    elif mssg.error():
                        raise KafkaException(mssg.error())
                else:
                    data.append(mssg.value())
                    if len(data) == 100:
                        if ETL(data):
                            data = list()
                            consumer.commit(
                                asynchronous=False
                            )
        finally:
            consumer.close()

    def extract(data: list[bytes]) -> list[json]:
        return list(json.loads(r) for r in data)

    def transform(data: list[json]) -> dict[str, int]:

        def qa_check(proc: dict) -> bool:
            num_rooms = proc.keys()
            if len(num_rooms) <= 3:
                return True
            else:
                return False

        if data:
            aggs = defaultdict(int)
            for r in data:
                aggs[r['room']] += r['count']
            if qa_check(aggs):
                return dict(aggs)

    def load(data: dict[str, int]) -> bool:
        if data:
            kwargs = dict(
                dbname=meta['db_name'],
                host=meta['db_host'],
                user=os.environ.get('USER', 'jameskirk'),
                password=os.environ.get('PW', '1b2b3'),
            )
            with psycopg2.connect(**kwargs) as conn:
                with conn.cursor() as curse:
                    table = 'room_and_counts'
                    for room, cnt in data.items():
                        curse.execute(
                            f'''
                            INSERT INTO {table} (room, count) 
                            VALUES ('{room}', {cnt})
                            ON CONFLICT (room)
                            DO
                                UPDATE
                                SET count = {table}.count + {cnt}
                                WHERE {table}.room = '{room}'
                            '''
                        )
                if curse.closed:
                    return True
                else:
                    return False

    def etl(data: list) -> bool:
        return (
            load(
                transform(
                    extract(data)
                )
            )
        )

    con(etl)


if __name__ == '__main__':

    etl_on_the_fly()
