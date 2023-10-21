FROM python:3.10

ADD . /k_moneyball

WORKDIR /k_moneyball

RUN pip install -r requirements.txt

RUN poetry install
RUN poetry env use python

CMD poetry run python data/init_mongodb.py