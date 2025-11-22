from confluent_kafka import Consumer, TopicPartition
from time import sleep
import json

painel = Consumer(
    {
        "bootstrap.servers": "kafka:9092",
        "group.id": "hotelaria",
        "auto.offset.reset": "earliest"
    }
)

while True:
    print(f"verificando hospedes")
    try:
        painel.assign([TopicPartition("reserva_finalizada", 0, 0)])
        hospede = painel.poll(timeout=2)
        while hospede:
            hospede = json.loads(hospede.value())
            print(f"dados do hospede: {hospede}")

            hospede = painel.poll(timeout=2)
    except Exception as erro:
        ...
    sleep(4)