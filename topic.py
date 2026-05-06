# Crear topic desde Python
from kafka.admin import KafkaAdminClient, NewTopic

admin = KafkaAdminClient(bootstrap_servers='kafka:9092')

topic = NewTopic(
    name='iot-events',
    num_partitions=3,
    replication_factor=1
)

admin.create_topics([topic])
print("Topic creado!")