#!/usr/bin/env python

import json
import os
import yaml
from collections import defaultdict
import psycopg2
from confluent_kafka import Consumer, KafkaException, KafkaError


ENV = os.environ['_ENV_']
if ENV == 'local':
    WORKDIR = f'{os.environ["HOME"]}/workspace/repos/samplepad'
else:
    WORKDIR = ''


def configs() -> dict:
    with open(f'{WORKDIR}/totalcounts/configs/specs.yaml') as confs:
        confs, port = yaml.safe_load(confs), '9092'
        confs.update(
            dict(
                servers=dict(
                    local=f'localhost:{port}',
                    docker_loc=f'host.internal.docker:{port}',
                )
            )
        )
        return confs


def extract(data: list[bytes]) -> list[json]:

    def is_valid(row: bytes) -> bool:
        try:
            json.loads(row)
        except (ValueError, TypeError):
            return False
        return True

    return list(
        json.loads(r) for r in data
        if is_valid(r)
    )


def transform(data: list[json]) -> dict[str, int]:

    def qa_check(proc: dict[str, int]) -> bool:
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

    def sql(table: str, room: str, cnt: int) -> str:
        return f'''
            INSERT INTO {table} (room, total_count) 
            VALUES ('{room}', {cnt})
            ON CONFLICT (room)
            DO
                UPDATE
                SET total_count = {table}.total_count + {cnt}
                WHERE {table}.room = '{room}'
        '''

    confs = configs()
    if data:
        kwargs = dict(
            dbname=confs['db_name'],
            host=confs['db_host'],
            user=os.environ.get('USER'),
            password=os.environ.get('PW'),
        )
        with psycopg2.connect(**kwargs) as conn:
            with conn.cursor() as curse:
                table = 'room_n_total_counts'
                for room, cnt in data.items():
                    if ENV == 'local':
                        print(room, cnt)
                    curse.execute(
                        sql(
                            table,
                            room,
                            cnt
                        )
                    )
            if curse.closed:
                return True
            else:
                return False


def subscriber(ETL: callable) -> None:
    confs = configs()
    con_conf = {
        'bootstrap.servers': confs['servers'].get(ENV, confs['boostrap_servers']),
        'group.id': confs['group_id'],
        'enable.auto.commit': False,
        'auto.offset.reset': confs['auto_offset_reset'],
    }
    data, consumer = list(), Consumer(con_conf)
    try:
        consumer.subscribe([confs['topic'], ])
        while True:
            mssg = consumer.poll(timeout=1.0)
            if mssg is None: continue
            if mssg.error():
                if mssg.error().code() in {KafkaError._PARTITION_EOF, }:
                    err = f'{mssg.topic()} EOF reached at {mssg.offset()}'
                    print(err)  # probably should use logging, eh?
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


def run() -> None:

    def etl(data: list) -> bool:

        def handles() -> bool:
            retry_cnt, transients = 0, (OSError, )
            while True:
                if retry_cnt > 2:
                    raise Exception('No dice!')
                try:
                    return (                        # This is what a ETL
                        load(                       # data pipeline should
                            transform(              # look like IMHO.
                                extract(data)       # Been heavily influenced by
                            )                       # functional programming.
                        )
                    )
                except transients:
                    retry_cnt += 1

        return handles()

    subscriber(etl)


if __name__ == '__main__':

    run()
