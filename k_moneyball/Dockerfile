FROM python:3.10

COPY requirements.txt .
COPY pyproject.toml .
COPY poetry.lock .

RUN pip install -r requirements.txt

ADD . /k_moneyball

WORKDIR /k_moneyball

CMD ["poetry", "install"]