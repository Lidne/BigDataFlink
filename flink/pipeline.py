from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import RuntimeExecutionMode, StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaSource,
)

import flink

sales_sink_ddl = """
CREATE TABLE pg_sink (
    id INT,
    processed_name STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://bigdata-postgres:5432/bigdata',
    'table-name' = 'data.',
    'username' = 'postgres',
    'password' = 'postgres',
    'driver' = 'org.postgresql.Driver'
)
"""

customers_sink_ddl = """
CREATE TABLE pg_sink (
    id INT,
    processed_name STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://bigdata-postgres:5432/bigdata',
    'table-name' = 'data.',
    'username' = 'postgres',
    'password' = 'postgres',
    'driver' = 'org.postgresql.Driver'
)
"""

sellers_sink_ddl = """
CREATE TABLE pg_sink (
    id INT,
    processed_name STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://bigdata-postgres:5432/bigdata',
    'table-name' = 'data.',
    'username' = 'postgres',
    'password' = 'postgres',
    'driver' = 'org.postgresql.Driver'
)
"""

products_sink_ddl = """
CREATE TABLE pg_sink (
    id INT,
    processed_name STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://bigdata-postgres:5432/bigdata',
    'table-name' = 'data.',
    'username' = 'postgres',
    'password' = 'postgres',
    'driver' = 'org.postgresql.Driver'
)
"""

stores_sink_ddl = """
CREATE TABLE pg_sink (
    id INT,
    processed_name STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://bigdata-postgres:5432/bigdata',
    'table-name' = 'data.',
    'username' = 'postgres',
    'password' = 'postgres',
    'driver' = 'org.postgresql.Driver'
)
"""

pets_sink_ddl = """
CREATE TABLE pg_sink (
    id INT,
    processed_name STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://bigdata-postgres:5432/bigdata',
    'table-name' = 'data.pets',
    'username' = 'postgres',
    'password' = 'postgres',
    'driver' = 'org.postgresql.Driver'
)
"""

products_sink_ddl = """
CREATE TABLE pg_sink (
    id INT,
    processed_name STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://bigdata-postgres:5432/bigdata',
    'table-name' = 'data.products',
    'username' = 'postgres',
    'password' = 'postgres',
    'driver' = 'org.postgresql.Driver'
)
"""

suppliers_sink_ddl = """
CREATE TABLE pg_sink (
    id INT,
    processed_name STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://bigdata-postgres:5432/bigdata',
    'table-name' = 'data.suppliers',
    'username' = 'postgres',
    'password' = 'postgres',
    'driver' = 'org.postgresql.Driver'
)
"""


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("kafka:9092")
        .set_topics("data")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")


if __name__ == "__main__":
    main()
