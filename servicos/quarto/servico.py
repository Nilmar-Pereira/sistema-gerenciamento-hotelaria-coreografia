from flask_apscheduler import APScheduler
from time import sleep
import json

from confluent_kafka import Consumer, Producer, TopicPartition

PROCESSO = "consulta_quartos"
PROCESSO_DE_RESERVA = "reserva_solicitada"
QUARTOS = "/servicos/quartos.json"

deslocamento = 0

def consultar_quartos(solicitacao):
    valido, tipo_quarto, mensagem, total_reserva = (solicitacao["sucesso"] == 1), "", "", 0.0
    if valido:
        with open(QUARTOS, "r") as arquivo_quartos:
            quarto = json.load(arquivo_quartos)
            quartos = quarto["quartos"]
            for quarto in quartos:
                print(quarto)
                if quarto["id"] == solicitacao["id_quarto"]:
                    tipo_quarto = quarto["tipo"]
                    valido = quarto["disponivel"] == True
                    if valido:
                        total_reserva = solicitacao["quantidade_diaria"] * float(quarto["preco_diaria"])
                        mensagem = "Quarto disponivel para reserva!" + solicitacao["identificacao"]
                    else:
                        mensagem = "O quarto já está reservado!"
                    break
            arquivo_quartos.close()
    else:
        mensagem = "Não foi possível consultar o quarto!"
    return valido, tipo_quarto, mensagem, total_reserva

def executar():
    global deslocamento
    consumidor = Consumer(
        {
            "bootstrap.servers": "kafka:9092",
            "group.id": "servico-quartos",
            "auto.offset.reset": "earliest"
        }
    )
    consumidor.assign([TopicPartition(PROCESSO_DE_RESERVA, 0, deslocamento)])

    produtor = Producer({
        "bootstrap.servers": "kafka:9092"
    })

    solicitacao = consumidor.poll(timeout=2.0)
    while solicitacao:
        deslocamento += 1

        solicitacao = solicitacao.value()
        solicitacao = json.loads(solicitacao)

        valido, tipo_quarto, mensagem, total_reserva = consultar_quartos(solicitacao)
        if valido:
            solicitacao["sucesso"] = 1
        else:
            solicitacao["sucesso"] = 0
        solicitacao["tipo"] = tipo_quarto
        solicitacao["mensagem"] = mensagem
        solicitacao["total_reserva"] = total_reserva

        produtor.produce(PROCESSO, key=solicitacao["identificacao"], value=json.dumps(solicitacao).encode("utf-8"))
        produtor.flush()
        solicitacao = consumidor.poll(timeout=2.0)

if __name__ == "__main__":
    agendador = APScheduler()
    agendador.add_job(id=PROCESSO, func=executar, trigger="interval", seconds=3)
    agendador.start()

    while True:
        sleep(5)