#!/usr/bin/env python

import json
import os
import boto3
import pyarrow
import yaml
from datetime import datetime
from io import StringIO
from uuid import uuid4
from confluent_kafka import Consumer, KafkaException, KafkaError
from pyarrow import parquet
from commons_cold.pathbuilder import PathBuilder


ENV = os.environ['_ENV_']
if ENV == 'local':
    WORKDIR = f'{os.environ["HOME"]}/workspace/repos/samplepad'
else:
    WORKDIR = ''


def configs() -> dict:
    with open(f'{WORKDIR}/storage/configs/specs.yaml') as confs:
        confs, port = yaml.safe_load(confs), '9092'
        confs.update(
            dict(
                servers=dict(
                    local=f'localhost:{port}',
                    docker=f'host.internal.docker:{port}',
                )
            )
        )
        return confs


def extract_as_parquet(data: list[bytes]) -> parquet:
    data = '\n'.join(list(json.loads(r) for r in data))
    table = (
        pyarrow
            .json
            .read_json(
                StringIO(data)
            )
    )
    return table


def load(data: parquet) -> bool:
    confs, uuid = configs(), str(uuid4()).replace('-', '')[0:9]
    bucket_name = confs['bucket']
    if data:
        s3 = boto3.client('s3')
        s3.put_object(
            data,
            bucket_name,
            PathBuilder()
                .bucket(bucket_name)
                .env('dv')
                .source('rooms')
                .subsource('midgar')
                .kind('raw')
                .ds(
                    str(
                        datetime
                            .utcnow()
                            .date()
                    )
                )
                .file_name(f'data-{uuid}')
                .blob()
        )
        return True
    return False


def subscriber(EL: callable) -> None:
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
                    print(err)
                elif mssg.error():
                    raise KafkaException(mssg.error())
            else:
                data.append(mssg.value())
                if len(data) == 500:
                    if EL(data):
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
                    return load(        # Can also do EL
                        extract(data)
                    )
                except transients:
                    retry_cnt += 1

        return handles()

    subscriber(etl)


if __name__ == '__main__':

    run()
