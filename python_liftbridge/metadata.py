from typing import NamedTuple


class MetaData(NamedTuple):
    brokers: list
    streams: list
    addr: list


class BrokerInfo(NamedTuple):
    id: str
    host: str
    port: int


class PartitionInfo(NamedTuple):
    id: int
    leader: BrokerInfo
    replicas: list
    isr: list


class StreamInfo(NamedTuple):
    subject: str
    name: str
    partitions: list


def find_broker_by_id(entries, id):
    return next(x for x in entries if x.id == id)

def find_partition_by_id(entries, id):
    return next(x for x in entries if x.id == id)


def find_broker_addr_of_leader(metadata, stream_name, partition):
    stream = next(x for x in metadata.streams if x.name == stream_name)
    # [TODO] find the parititon by index
    partition_of_stream = find_partition_by_id(stream.partitions, partition)
    leader = partition_of_stream.leader
    leader_info = find_broker_by_id(metadata.brokers, leader)
    return leader_info.host + ":" + str(leader_info.port)


def generate_meta_data(meta_data_response):
    streams = []
    # Broker
    brokers = []
    addr = []
    for broker in meta_data_response.brokers:
        b = BrokerInfo(id=broker.id, port=broker.port, host=broker.host)
        brokers.append(b)
        addr.append(b.host + ":" + str(b.port))

    for stream_meta in meta_data_response.metadata:
        partitions = []

        for _, p in stream_meta.partitions.items():
            replicas = [find_broker_by_id(brokers, r) for r in p.replicas]
            isr = [find_broker_by_id(brokers, r) for r in p.isr]
            partition_info = PartitionInfo(id=p.id,
                                           leader=p.leader,
                                           replicas=replicas,
                                           isr=isr)
            partitions.append(partition_info)
        stream = StreamInfo(stream_meta.subject, stream_meta.name, partitions)
        streams.append(stream)
    return MetaData(brokers=brokers, streams=streams, addr=addr)