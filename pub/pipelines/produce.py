#!/usr/bin/env python

import hashlib
import json
import yaml
from datetime import datetime
from random import randint
from confluent_kafka import Message, Producer


def metadata() -> dict:
    with open('/pub/configs/specs.yaml') as confs:
        return yaml \
            .safe_load(
                confs
            )


def make_data() -> None:

    meta = metadata()
    prod_conf = {
        'bootstrap.servers': meta['boostrap_servers'],
        'delivery.timeout.ms': meta['delivery_timeout_ms'],
    }
    rooms = {1: 'A', 2: 'B', 3: 'C', }

    def ack(error, mssg: Message) -> None:
        mssg = str(mssg.value())
        if error is not None:
            print(f'Failed to deliver message: {mssg}: {str(error)}')
        else:
            print(f'Message produced: {mssg}')

    def make_hash(mssg: str) -> str:
        return (
            hashlib
                .sha256(mssg.encode('utf-8'))
                .hexdigest()
        )

    def handle_transient_errors(producer: Producer, mssg: str, retry_cnt: int) -> None:
        if retry_cnt > 2: raise
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

    def pub() -> None:
        prod = Producer(prod_conf)
        for _ in range(100):
            mssg = json.dumps(
                dict(
                    ts=str(datetime.utcnow()),
                    room=rooms[randint(1, 3)],
                    count=randint(0, 9)
                )
            )
            handle_transient_errors(
                prod,
                mssg=mssg,
                retry_cnt=0
            )
            prod.poll(0.01)
        prod.flush()

    pub()


if __name__ == '__main__':

    make_data()
