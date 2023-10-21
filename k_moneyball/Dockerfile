FROM python:3.10

RUN pip install -r requirements.txt

ADD . /k_moneyball

WORKDIR /k_moneyball

RUN poetry install
RUN poetry env use python3