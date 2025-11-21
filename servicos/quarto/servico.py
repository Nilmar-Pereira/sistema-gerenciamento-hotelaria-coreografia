from flask_apscheduler import APScheduler
from time import sleep
import json

from confluent_kafka import Consumer, Producer, TopicPartition

PROCESSO = "reserva_quartos"
PROCESSO_DE_RESERVA = "reserva_hospede"
QUARTOS = "/servicos/quartos.json"

deslocamento = 0

def validar_reservas(solicitacao_reserva):
    valido, mensagem, total_reserva = (solicitacao_reserva["sucesso"] == 1), "", 0.0
    if valido:
        with open(QUARTOS, "r") as arquivo_reservas:
            quarto = json.load(arquivo_reservas)
            reservas = quarto["quartos"]
            for reserva in reservas:
                print(reserva)
                if reserva["id"] == solicitacao_reserva["id_quarto"]:
                   # tipo = quarto["tipo"]
                    valido = (solicitacao_reserva["disponivel"] == quarto["disponivel"])
                    if valido:
                        total_reserva = solicitacao_reserva["quantidade_diaria"] * float(quarto["preco_diaria"])
                        mensagem = "Quarto reservado com sucesso #" + solicitacao_reserva["identificacao"]
                    else:
                        mensagem = "O quarto já está reservado!"
                    break
            arquivo_reservas.close()
    else:
        mensagem = "A reserva do quarto não foi efetuada!"
    return valido, mensagem, total_reserva

def executar():
    global deslocamento
    consumidor = Consumer(
        {
            "bootstrap.servers": "kafka:9092",
            "group.id": "hotelaria",
            "auto.offset.reset": "earliest"
        }
    )
    consumidor.assign([TopicPartition(PROCESSO_DE_RESERVA, 0, deslocamento)])

    produtor = Producer({
        "bootstrap.servers": "kafka:9092"
    })

    reserva = consumidor.poll(timeout=2.0)
    while reserva:
        deslocamento += 1

        reserva = reserva.value()
        reserva = json.loads(reserva)

        valido, mensagem, total_reserva = validar_reservas(reserva)
        if valido:
            reserva["sucesso"] = 1
        else:
            reserva["sucesso"] = 0
        #reserva["tipo"] = tipo
        reserva["mensagem"] = mensagem
        reserva["total_reserva"] = total_reserva

        produtor.produce(PROCESSO, key=reserva["identificacao"], value=json.dumps(reserva).encode("utf-8"))
        produtor.flush()
        reserva = consumidor.poll(timeout=2.0)

if __name__ == "__main__":
    agendador = APScheduler()
    agendador.add_job(id=PROCESSO, func=executar, trigger="interval", seconds=3)
    agendador.start()

    while True:
        sleep(5)