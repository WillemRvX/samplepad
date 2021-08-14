#!/usr/bin/env python

import hashlib
import json
import os
import yaml
from datetime import datetime
from random import randint
from time import sleep
from confluent_kafka import Message, Producer


ENV = os.environ['_ENV_']
if ENV == 'local':
    WORKDIR = f'{os.environ["HOME"]}/workspace/repos/samplepad'
else:
    WORKDIR = ''


def config() -> dict:
    with open(f'{WORKDIR}/gendata/configs/specs.yaml') as confs:
        confs, port = yaml.safe_load(confs), '9092'
        confs.update(
            dict(
                servers=dict(
                    local=f'localhost:{port}',
                    dock_loc=f'host.docker.internal:{port}',
                )
            )
        )
        return confs


def make_hash(mssg: str) -> str:
    return (
        hashlib
            .sha256(mssg.encode('utf-8'))
            .hexdigest()
    )


def ack(error, mssg: Message) -> None:
    mssg = str(mssg.value())
    if error is not None:
        print(f'Failed to deliver message: {mssg}: {str(error)}')
    else:
        print(f'Message produced: {mssg}')


def handles(producer: Producer, mssg: str, retry_cnt: int) -> None:
    if retry_cnt > 2: raise Exception('No dice!')
    confs, transients = config(), (OSError, )
    kwargs = dict(
        topic=confs['topic'],
        key=make_hash(mssg),
        value=mssg,
        callback=ack,
    )
    try:
        producer.produce(**kwargs)
    except transients:
        handles(
            producer,
            mssg,
            retry_cnt=retry_cnt+1
        )


def run() -> None:

    confs = config()
    prod_conf = {
        'bootstrap.servers': confs['servers'].get(ENV, confs['boostrap_servers']),
        'delivery.timeout.ms': confs['delivery_timeout_ms'],
    }
    rooms = {1: 'A', 2: 'B', 3: 'C', }

    def publisher() -> None:
        prod = Producer(prod_conf)
        for _ in range(10000000):
            mssg = json.dumps(
                dict(
                    ts=str(datetime.utcnow()),
                    room=rooms[randint(1, 3)],
                    count=4 if rooms[randint(1, 3)] == 'A' else 1
                )
            )
            handles(
                prod,
                mssg=mssg,
                retry_cnt=0
            )
            prod.poll(0.01)
            sleep(0.01)
        prod.flush()

    publisher()


if __name__ == '__main__':

    run()
