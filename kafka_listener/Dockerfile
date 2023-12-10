FROM python:3.10

ADD . /kafka_listener

WORKDIR /kafka_listener

RUN pip install -r requirements.txt

RUN poetry install
RUN poetry env use python

CMD poetry run python kafka_listener/kafka_listener.py
# CMD tail -f /dev/null