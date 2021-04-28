# Copyright (c) 2020 Aiven, Helsinki, Finland. https://aiven.io/

from .object_storage.gcs import GCSProvider
from argparse import ArgumentParser
from tempfile import TemporaryDirectory

import codecs
import datetime
import dateutil
import gzip
import json
import kafka
import logging
import os
import re


class KafkaRestore:
    def __init__(self, *, config):
        self.log = logging.getLogger(self.__class__.__name__)
        self.config = config

        object_storage_config = self.config.get("object_storage", {})
        object_storage_type = object_storage_config.get("type")
        if object_storage_type == "gcs":
            self.object_storage = GCSProvider(config=object_storage_config)
        else:
            raise ValueError(f"Unknown object storage type: {object_storage_type}")

        kafka_config = self.config.get("kafka", {})

        if ("ssl_cafile" in kafka_config and
                "ssl_access_certificate_file" in kafka_config and
                "ssl_access_key_file" in kafka_config):
            self.kafka_producer = kafka.KafkaProducer(
                bootstrap_servers=kafka_config["kafka_url"],
                security_protocol="SSL",
                ssl_cafile=kafka_config["ssl_ca_file"],
                ssl_certfile=kafka_config["ssl_access_certificate_file"],
                ssl_keyfile=kafka_config["ssl_access_key_file"],
            )
        else:
            self.kafka_producer = kafka.KafkaProducer(
                bootstrap_servers=kafka_config["kafka_url"],
            )

    def list_topic_data_files(self, *, topic):
        topic_re = re.compile(
            (
                r"(?P<topic>" + re.escape(topic) + r")"
                r"-(?P<partition>[0-9]+)"
                r"-(?P<offset>[0-9]+)"
                r"(?P<suffix>[.a-z]*)"
            )
        )

        topic_partition_files = {}

        for item in self.object_storage.list_items():
            matches = topic_re.match(item.name)
            if matches:
                partition = int(matches.group("partition"))
                if partition not in topic_partition_files:
                    topic_partition_files[partition] = []
                begin_offset = matches.group("offset")
                record = {
                    "begin_offset": int(begin_offset),
                    "last_modified": item.last_modified,
                    "object_name": item.name,
                }
                if matches.group("suffix") == ".gz":
                    record["compression"] = "gzip"
                topic_partition_files[partition].append(record)

        for partition in topic_partition_files:
            topic_partition_files[partition] = sorted(topic_partition_files[partition], key=lambda x: x["begin_offset"])

        return topic_partition_files

    def parse_record(self, record_line):
        fields = record_line.split(",")
        if fields[0]:
            key = codecs.decode(codecs.encode(fields[0], "ascii"), "base64")
        else:
            key = None
        if fields[1]:
            value = codecs.decode(codecs.encode(fields[1], "ascii"), "base64")
        else:
            value = None
        offset = int(fields[2])
        if fields[3]:
            timestamp = int(fields[3])
        else:
            timestamp = None

        return key, value, offset, timestamp

    def restore(self, *, topic):
        topic_partition_files = self.list_topic_data_files(topic=topic)
        partition_offset_records = {}
        since = self.config.get("since")

        with TemporaryDirectory() as working_directory:
            while True:
                progress = False
                for partition in topic_partition_files:
                    if topic_partition_files[partition]:
                        object_record = topic_partition_files[partition][0]
                        topic_partition_files[partition] = topic_partition_files[partition][1:]
                        progress = True

                        object_name = object_record["object_name"]

                        if since is not None and since > object_record["last_modified"]:
                            self.log.info("Skipping object %r due to timestamp", object_name)
                            continue

                        local_name = f"{working_directory}/{topic}-{partition}"
                        self.object_storage.get_contents_to_file(object_name, local_name)

                        if object_record.get("compression") == "gzip":
                            fh = gzip.open(local_name, "rt")
                        else:
                            fh = open(local_name, "r")

                        nrecords = 0
                        for line in fh.readlines():
                            key, value, offset, timestamp = self.parse_record(line.strip())
                            future_record = self.kafka_producer.send(
                                topic,
                                partition=partition,
                                key=key,
                                value=value,
                                timestamp_ms=timestamp,
                            )
                            nrecords += 1
                            partition_offset_records[partition] = {
                                "last_original_offset": offset,
                                "last_produced_record": future_record,
                            }

                        self.log.info("Restored %d messages from object %r", nrecords, object_name)

                        fh.close()
                        os.unlink(local_name)

                if not progress:
                    self.kafka_producer.flush()
                    break

        for partition in sorted(partition_offset_records):
            self.log.info(
                "Partition %d original offset %d new offset %d",
                partition,
                partition_offset_records[partition]["last_original_offset"],
                partition_offset_records[partition]["last_produced_record"].get().offset,
            )


def main():
    logging.basicConfig(level=logging.INFO, format="%(name)-20s  %(levelname)-8s  %(message)s")

    parser = ArgumentParser()
    parser.add_argument("-c", "--config", required=True, help="Path to config file")
    parser.add_argument("-t", "--topic", required=True, help="Topic name")
    parser.add_argument("--since", help="Skip objects that are older than given timestamp")
    args = parser.parse_args()

    with open(args.config) as fh:
        restore_config = json.load(fh)

    if args.since:
        dt = dateutil.parser.parse(args.since)
        if dt.tzinfo is None:
            # assume UTC if no timezone is present
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        restore_config["since"] = dt

    kafka_restore = KafkaRestore(config=restore_config)

    kafka_restore.restore(topic=args.topic)


if __name__ == "__main__":
    main()
