from flask_apscheduler import APScheduler
from time import sleep
import json

from confluent_kafka import Consumer, Producer, TopicPartition

PROCESSO = "reserva_finalizada"
PROCESSO_DE_CONSULTA_DE_QUARTOS = "consulta_quartos"
HOSPEDES = "/servicos/hospedes.json"

deslocamento = 0

def validar_hospede(solicitacao):
    valido = (solicitacao["sucesso"] == 1)
    nome_hospede, email_hospede, mensagem = "", "", ""
    
    if not valido:
        mensagem = ("Não é possível validar o hóspede, pois o quarto não está disponível.")
        return False, nome_hospede, email_hospede, mensagem
        
    with open(HOSPEDES, "r") as arquivo_hospedes:
        hospede = json.load(arquivo_hospedes)
        hospedes = hospede["hospedes"]
        encontrado = False
        for hospede in hospedes:
            print(hospede)
            if hospede["id"] == solicitacao["id_hospede"]:
                encontrado = True
                nome_hospede = hospede["nome"]
                email_hospede = hospede["email"]
                
                # valida pendências
                if hospede["pendencia"] == False:
                    mensagem = "Hóspede validado com sucesso! ID da reserva: " + solicitacao["identificacao"]
                    valido = True
                else:
                    mensagem = "O hóspede possui pendências, portanto a reserva não pode ser concluída."
                    valido = False
                break
        arquivo_hospedes.close()
        if not encontrado:
            mensagem = "Hóspede não encontrado no cadastro."
            valido = False
    return valido, nome_hospede, email_hospede, mensagem

def executar():
    global deslocamento
    consumidor = Consumer(
        {
            "bootstrap.servers": "kafka:9092",
            "group.id": "hotelaria",
            "auto.offset.reset": "earliest"
        }
    )
    consumidor.assign([TopicPartition(PROCESSO_DE_CONSULTA_DE_QUARTOS, 0, deslocamento)])

    produtor = Producer({
        "bootstrap.servers": "kafka:9092"
    })

    solicitacao = consumidor.poll(timeout=2.0)
    while solicitacao:
        deslocamento += 1

        solicitacao = solicitacao.value()
        solicitacao = json.loads(solicitacao)

        valido, nome_hospede, email_hospede, mensagem = validar_hospede(solicitacao)
        if valido:
            solicitacao["sucesso"] = 1
        else:
            solicitacao["sucesso"] = 0
            
        solicitacao["nome"] = nome_hospede
        solicitacao["email"] = email_hospede
        solicitacao["mensagem"] = mensagem

        produtor.produce(PROCESSO, key=solicitacao["identificacao"], value=json.dumps(solicitacao).encode("utf-8"))
        produtor.flush()
        solicitacao = consumidor.poll(timeout=2.0)

if __name__ == "__main__":
    agendador = APScheduler()
    agendador.add_job(id=PROCESSO, func=executar, trigger="interval", seconds=3)
    agendador.start()

    while True:
        sleep(5)