# Aiven Restore Utility for Apache Kafka®

Utility for restoring Apache Kafka topic data backed up with [Aiven Kafka GCS Connector](https://github.com/aiven/aiven-kafka-connect-gcs)

[GitHub repository](https://github.com/aiven/aiven-kafka-restore)

## How It Works

This tool can download and restore Apache Kafka messages stored in GCS by Aiven Kafka GCS Connector.

## Limitations

The are current limitations with the tool:

  * The restored topic needs to exists in the desired configuration, including partition count.
  * The tool supports the default filename format only (`<topic>-<partition>-<start_offset>`). Prefix for subdirectories is supported, however.
  * The tool supports the default record format only (`key,value,offset,timestamp`).
  * The tool is unable to restore consumer group offsets, but does output offset difference between the original and the new cluster. This difference can be used to adjust consumers to the correct location, but does require manual work.

We plan to address these shortcomings in the near future.

## Usage

`python3 -m kafka_restore -c <configuration_file> -t <topic> [--since <timestamp>]`

Argument `since` can be used to limit application of restore to recent objects only. The timestamp can be date (`2020-06-09`) or date and time (`2020-06-09 12:00+00:00`). Using this flag can speed up recovery for setups where object storage contains the full history of topic data.

## Configuration file format

Example configuration file contents are as follows:

```
{
    "kafka": {
        "kafka_url": "kafka-example.aivencloud.com:11397",
        "ssl_ca_file": "ca.crt",
        "ssl_access_certificate_file": "service.crt",
        "ssl_access_key_file": "service.key"
    },
    "object_storage": {
        "type": "gcs",
        "bucket": "example-backup-bucket",
        "credentials_file": "example-gcs-credentials.json"
    }
}
```

Optional prefix for subdirectories can be specified using `prefix` key under the object storage config.

## Restore procedure

  1. Stop all consumer and produced pipelines.
  2. Re-create the topic on a new cluster. The configuration must match the original topic in number of partitions.
  3. Run the restore tool to recover topic data. Record the offset differences between the original and the new cluster.
  4. Perform the above steps for all topics one wants to restore.
  5. Either configure consumers (re-)start from the latest offset.
  6. If consumer location needs to be adjusted and one has the offset information, the offset difference can be applied to adjust consumer start location.
  7. Once the consumers are running, you can (re-)enable producers as well.

## Trademarks

Apache Kafka, Apache Kafka Connect are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries.
