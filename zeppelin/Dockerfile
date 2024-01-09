FROM apache/zeppelin:0.10.1

ENV SPARK_MASTER=spark://spark-master:7077
ENV SPARK_HOME=/opt/bitnami/spark
ENV ZEPPELIN_LOG_DIR=/logs
ENV ZEPPELIN_PORT=9999


ENV POST_URL https://jdbc.postgresql.org/download/postgresql-42.7.1.jar
RUN wget ${POST_URL}

COPY ./conf/zeppelin-env.sh /opt/zeppelin/conf/
COPY ./interpreter/interpreter.json /opt/zeppelin/conf/
