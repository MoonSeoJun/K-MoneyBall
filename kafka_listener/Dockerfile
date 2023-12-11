FROM python:3.10

ADD . /kafka_listener

WORKDIR /kafka_listener

RUN pip install -r requirements.txt

CMD ./run.sh