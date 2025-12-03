from confluent_kafka import Consumer, TopicPartition
from time import sleep
import json

painel = Consumer(
    {
        "bootstrap.servers": "kafka:9092",
        "group.id": "servico-quartos",
        "auto.offset.reset": "earliest"
    }
)

while True:
    print(f"verificando quartos")
    try:
        painel.assign([TopicPartition("consulta_quartos", 0, 0)])
        quarto = painel.poll(timeout=2)
        while quarto:
            quarto = json.loads(quarto.value())
            print(f"dados do quarto: {quarto}\n")

            quarto = painel.poll(timeout=2)
    except Exception as erro:
        ...
    sleep(4)