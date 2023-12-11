import psycopg2

import logging

class PostgresqlConnector:
    def __init__(self) -> None:
        self.conn = psycopg2.connect(database="k_moneyball",
                                     user="k_moneyball",
                                     password="k_moneyball",
                                     host="postgres_kmoneyball",
                                     port="5432")
        
        logging.info(self.conn.info)