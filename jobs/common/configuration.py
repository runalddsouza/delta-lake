import argparse
import sys
from dataclasses import dataclass


@dataclass
class Configuration:
    partition_keys: int
    key: str
    partitions: int
    input: str
    prefix: str
    table: str
    partition_key = 'partition_id'


def table_load_config() -> Configuration:
    parser = argparse.ArgumentParser()
    parser.add_argument("--partitionkeys", "-p", required=True)
    parser.add_argument("--partitions", "-f", required=True)
    parser.add_argument("--key", "-k", required=True)
    parser.add_argument("--input", "-i", required=True)
    parser.add_argument("--outputprefix", "-o", required=True)
    parser.add_argument("--table", "-t", required=True)
    args = parser.parse_args()

    if len(sys.argv) < 6:
        parser.print_help()
        sys.exit(-1)

    return Configuration(int(args.partitionkeys), args.key, int(args.partitions), args.input, args.outputprefix,
                         args.table)


def table_stream_config() -> Configuration:
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", required=True, help="Kafka topic")
    parser.add_argument("--servers", required=True, help="Bootstrap server")
    parser.add_argument("--outputprefix", "-o", required=True)
    parser.add_argument("--key", "-k", required=True)
    parser.add_argument("--partitionkeys", "-p", required=True)
    args = parser.parse_args()

    if len(sys.argv) < 4:
        parser.print_help()
        sys.exit(-1)

    return Configuration(int(args.partitionkeys), args.key, -1, args.servers, args.outputprefix, args.topic)
