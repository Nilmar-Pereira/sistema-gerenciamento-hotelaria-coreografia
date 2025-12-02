🏨 Sistema de Gerenciamento de Reservas de Hotel
Arquitetura Distribuída Coreografada com Apache Kafka + Docker

Este projeto implementa um fluxo distribuído de reservas de hotel utilizando microserviços independentes, que se comunicam exclusivamente através de eventos Kafka, adotando o padrão de coreografia.
Cada serviço roda em seu próprio container Docker, garantindo isolamento e escalabilidade.

📌 Arquitetura Geral

A solução é composta por 3 microserviços principais, além do broker Kafka:

Cliente → Serviço de Reserva → Serviço de Quartos → Serviço de Hóspedes → Saída final

✔ Serviço 1 — reserva_solicitada

Recebe solicitações HTTP e publica no Kafka.

✔ Serviço 2 — consulta_quartos

Consome pedidos de reserva e valida disponibilidade de quartos.

✔ Serviço 3 — reserva_finalizada

Valida informações do hóspede e finaliza a reserva.

🧩 Tecnologias Utilizadas

Python 3

Flask

APScheduler

Apache Kafka (Confluent Platform)

Docker + Docker Compose

confluent-kafka-python

JSON como “banco de dados local”

🐳 Como Executar o Projeto

Certifique-se de ter instalado:

Docker

Docker Compose

▶️ Iniciar todos os serviços
docker compose up -d --build


Isso irá subir:

Kafka

Zookeeper

Serviço de reserva

Serviço de quarto

Serviço de hospede

📬 Como Enviar uma Solicitação de Reserva

Após os containers estarem rodando:

🎯 Endpoint:
POST http://localhost:5001/reserva

📦 Exemplo de JSON:
{
    "id_quarto": 1,
    "id_hospede": 2,
    "quantidade_diaria": 3
}


