FROM python:3.10

ADD . /k_moneyball

WORKDIR /k_moneyball

RUN pip install -r requirements.txt

RUN poetry install
RUN poetry env use python

CMD poetry run uvicorn main:app --host 0.0.0.0 --port 80