
from confluent_kafka import Producer
from confluent_kafka import Consumer


def _get_default_producer_config(
    bootstrap_servers: str,
    client_id:str,
) -> dict:

    config = {
        'bootstrap.servers': bootstrap_servers,
        'client.id': client_id,
        'enable.idempotence': True,
    }

    return config


def _get_default_consumer_config(
    bootstrap_servers: str,
    client_id:str,
    group_id:str,
) -> dict:

    config = {
        'bootstrap.servers': bootstrap_servers,
        'client.id': client_id,
        'group.id': group_id,
        'enable.auto.commit': False,
        'auto.offset.reset': 'earliest',
        'isolation.level': 'read_committed',
    }

    return config


def create_producer(
    bootstrap_servers: str,
    client_id: str,
) -> Producer:

    config = _get_default_producer_config(bootstrap_servers, client_id)
    producer = Producer(config)
    return producer


def create_consumer(
    bootstrap_servers: str,
    client_id:str,
    group_id:str,
) -> Consumer:

    config = _get_default_consumer_config(bootstrap_servers, client_id, group_id)
    consumer = Consumer(config)
    return consumer
