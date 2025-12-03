from flask import Flask, Response, request

from confluent_kafka import Producer

import hashlib
import random
import string
import json

PROCESSO = "reserva_solicitada"
INFO = {
    "descricao": "Serviço que solicita a reserva de hóspedes",
    "versao": "0.0.1"
}

servico = Flask(PROCESSO)

@servico.get("/")
def get_info():
    return Response(json.dumps(INFO), status = 200, mimetype="application/json")

@servico.post("/reserva")
def reservar_hospede():
    sucesso = False
    ID = "".join(random.choice(string.ascii_letters + string.punctuation) for _ in range(16))
    ID = hashlib.md5(ID.encode("utf-8")).hexdigest()
    dados = request.json
    try:
        reserva = {
            "identificacao": ID, 
            "sucesso": 1,
            "mensagem": "reserva iniciada",
            "id_quarto": dados["id_quarto"],
            "id_hospede": dados["id_hospede"],
            "quantidade_diaria": dados["quantidade_diaria"]
        }

        produtor = Producer({"bootstrap.servers" : "kafka:9092"})
        produtor.produce(PROCESSO, key=ID, value=json.dumps(reserva).encode("utf-8"))
        produtor.flush()

        sucesso = True
    except Exception as erro:
        print(f"Um erro ocorreu durante a reserva: {erro}")

    return Response(status = 201 if sucesso else 422)

if __name__ == "__main__":
    servico.run(host="0.0.0.0", debug=True)