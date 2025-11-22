FROM python:latest

RUN pip install Flask
RUN pip install requests
RUN pip install confluent_kafka
RUN pip install flask_apscheduler

RUN mkdir /servicos
WORKDIR /servicos