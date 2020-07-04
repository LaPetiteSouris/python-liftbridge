# -*- coding: utf-8 -*-
from python_liftbridge.api_pb2 import FetchMetadataResponse, Broker, \
    StreamMetadata, PartitionMetadata
from python_liftbridge.metadata import MetaData

META_DATA = {
    "brokers": [{
        "id": "32opSXdJQuGQAF2shypjDB",
        "host": "localhost",
        "port": 9292
    }, {
        "id": "32opSXdJQuGQAF2shkpk",
        "host": "localhost",
        "port": 9293
    }],
    "metadata": [{
        "name":
        "test-stream",
        "subject":
        "test",
        "partitions": [{
            "id":
            0,
            "leader":
            "32opSXdJQuGQAF2shypjDB",
            "replicas": ["32opSXdJQuGQAF2shypjDB", "32opSXdJQuGQAF2shkpk"],
            "isr": ["32opSXdJQuGQAF2shypjDB", "32opSXdJQuGQAF2shkpk"]
        }, {
            "id":
            1,
            "leader":
            "32opSXdJQuGQAF2shkpk",
            "replicas": ["32opSXdJQuGQAF2shypjDB", "32opSXdJQuGQAF2shkpk"],
            "isr": ["32opSXdJQuGQAF2shypjDB", "32opSXdJQuGQAF2shkpk"]
        }]
    }]
}


class TestMetaData(object):
    def setup_method(self):
        meta_data_response = FetchMetadataResponse()
        brokers = [
            Broker(id=broker.get("id"),
                   host=broker.get("host"),
                   port=broker.get("port")) for broker in META_DATA["brokers"]
        ]

        meta_data_response.brokers.extend(brokers)

        metadata = []

        for m in META_DATA["metadata"]:
            stream_meta_data = StreamMetadata(name=m.get("name"),
                                              subject=m.get("subject"))
            for p in m["partitions"]:
                partition_meta_data = PartitionMetadata(id=p["id"],
                                                        leader=p["leader"])
                partition_meta_data.isr.extend(p["isr"])
                partition_meta_data.isr.extend(p["replicas"])

                stream_meta_data.partitions[p["id"]].CopyFrom(
                    partition_meta_data)
            metadata.append(stream_meta_data)
        meta_data_response.metadata.extend(metadata)
        self.meta_data_response = meta_data_response

    def test_parse_leader(self):
        meta_data_cache = MetaData()
        meta_data_cache.refresh_meta_data(self.meta_data_response)
        # partition 0
        leader_addr = meta_data_cache.find_broker_addr("test-stream", 0, False)
        assert leader_addr == "localhost:9292"
        # partition 1
        leader_addr = meta_data_cache.find_broker_addr("test-stream", 1, False)
        assert leader_addr == "localhost:9293"

    def test_parse_isr(self):
        meta_data_cache = MetaData()
        meta_data_cache.refresh_meta_data(self.meta_data_response)

        # partition 0
        addr = meta_data_cache.find_broker_addr("test-stream", 0, True)
        assert addr in ["localhost:9292", "localhost:9293"]

        # partition 1
        addr = meta_data_cache.find_broker_addr("test-stream", 1, False)
        assert addr in ["localhost:9292", "localhost:9293"]