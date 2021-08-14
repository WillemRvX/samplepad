#!/usr/bin/env python

import json
import os
import boto3
import pyarrow
import yaml
from datetime import datetime
from io import BytesIO
from uuid import uuid4
from confluent_kafka import Consumer, KafkaException, KafkaError
from pyarrow import json as pyarrow_json, parquet
from commons.utils import PathBuilder


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
                    docker_loc=f'host.internal.docker:{port}',
                )
            )
        )
        return confs


def extract(data: list[bytes]) -> bytes:
    data = '\n'.join(list(r.decode('utf-8') for r in data))
    table = (
        pyarrow_json
            .read_json(
                BytesIO(
                    data.encode('utf-8')
                )
            )
    )
    writer = pyarrow.BufferOutputStream()
    parquet.write_table(table, writer)
    return bytes(
        writer.getvalue()
    )


def load(data: bytes) -> bool:
    confs, uuid = configs(), str(uuid4()).replace('-', '')[0:9]
    bucket_name, fname = confs['bucket'], f'data-{uuid}.parquet'
    if data:
        s3 = boto3.client('s3')
        s3.put_object(
            Body=data,
            Bucket=bucket_name,
            Key=PathBuilder()                       # I used to code in Scala.
                .bucket(bucket_name, cloud='aws')   # Chained   method   calls
                .env('dv')                          # Kinda grew on me.   What
                .source('rooms')                    # can I say?  -Will
                .subsource('midgar')
                .kind('raw')
                .ds(
                    str(
                        datetime
                            .utcnow()
                            .date()
                    )
                )
                .file_name(fname)
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
                print(mssg.value())
                if len(data) == 1000:
                    if EL(data):
                        data = list()
                        consumer.commit(
                            asynchronous=False
                        )
    finally:
        consumer.close()


def run() -> None:

    def el(data: list) -> bool:

        def handles() -> bool:
            retry_cnt, transients = 0, (OSError, )
            while True:
                if retry_cnt > 2:
                    raise Exception('No dice!')
                try:
                    return (
                        load(                   # Can also do EL
                            extract(data)
                        )
                    )
                except transients:
                    retry_cnt += 1

        return handles()

    subscriber(el)


if __name__ == '__main__':

    run()
