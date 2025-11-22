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
    print(f"verificando reservas")
    try:
        painel.assign([TopicPartition("reserva_solicitada", 0, 0)])
        reserva = painel.poll(timeout=2)
        while reserva:
            reserva = json.loads(reserva.value())
            print(f"dados da reserva: {reserva}")

            reserva = painel.poll(timeout=2)
    except Exception as erro:
        ...
    sleep(4)