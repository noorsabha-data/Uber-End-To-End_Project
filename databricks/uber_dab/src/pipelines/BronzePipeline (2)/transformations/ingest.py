from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *


# ============================================================
# Event Hubs configuration
# ============================================================

EH_NAMESPACE = "ubereventsnoor"
EH_NAME = "noorubertopic"

# Get Event Hub connection string from Databricks Secret Scope
EH_CONN_STR = dbutils.secrets.get(
    scope="uber-eventhub",
    key="connection-string"
)


KAFKA_OPTIONS = {
    "kafka.bootstrap.servers": f"{EH_NAMESPACE}.servicebus.windows.net:9093",
    "subscribe": EH_NAME,
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.jaas.config": (
        'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule '
        'required username="$ConnectionString" '
        f'password="{EH_CONN_STR}";'
    ),
    "kafka.request.timeout.ms": "10000",
    "kafka.session.timeout.ms": "10000",
    "maxOffsetsPerTrigger": "10000",
    "failOnDataLoss": "true",
    "startingOffsets": "earliest"
}


# ============================================================
# Bronze streaming table
# ============================================================

@dp.table
def rides_raw():

    df = (
        spark.readStream
        .format("kafka")
        .options(**KAFKA_OPTIONS)
        .load()
    )

    # Convert Kafka value from binary to string
    df = df.withColumn(
        "rides",
        col("value").cast("string")
    )

    return df