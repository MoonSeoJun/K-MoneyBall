FROM apache/zeppelin:0.10.1

ENV ZEPPELIN_SPARK_MASTER=spark://spark:7077
ENV SPARK_HOME=/usr/spark-3.2.0
ENV ZEPPELIN_LOG_DIR=/logs
ENV ZEPPELIN_PORT=9999

COPY ./interpreter/interpreter.json /opt/zeppelin/conf/