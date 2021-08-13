#!/usr/bin/env python

import hashlib
import json
import yaml
from datetime import datetime
from random import randint
from time import sleep
from confluent_kafka import Message, Producer


def config() -> dict:
    with open('/pub/configs/specs.yaml') as confs:
        return yaml \
            .safe_load(
                confs
            )


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


def handle_transient_errors(producer: Producer, mssg: str, retry_cnt: int) -> None:
    if retry_cnt > 2: raise Exception('No dice!')
    meta = config()
    kwargs = dict(
        topic=meta['topic'],
        key=make_hash(mssg),
        value=mssg,
        callback=ack,
    )
    try:
        producer.produce(**kwargs)
    except OSError:
        handle_transient_errors(
            producer,
            mssg,
            retry_cnt=retry_cnt+1
        )


def make_data() -> None:

    meta = config()
    prod_conf = {
        'bootstrap.servers': meta['boostrap_servers'],
        'delivery.timeout.ms': meta['delivery_timeout_ms'],
    }
    rooms = {1: 'A', 2: 'B', 3: 'C', }

    def publish() -> None:
        prod = Producer(prod_conf)
        for _ in range(10000000):
            mssg = json.dumps(
                dict(
                    ts=str(datetime.utcnow()),
                    room=rooms[randint(1, 3)],
                    count=4 if rooms[randint(1, 3)] == 'A' else 1
                )
            )
            handle_transient_errors(
                prod,
                mssg=mssg,
                retry_cnt=0
            )
            prod.poll(0.01)
            sleep(0.01)
        prod.flush()

    publish()


if __name__ == '__main__':

    make_data()
