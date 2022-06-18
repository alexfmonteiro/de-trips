FROM jupyter/pyspark-notebook

RUN pip3 install psycopg2-binary
ADD https://jdbc.postgresql.org/download/postgresql-42.2.18.jar /usr/local/spark/jars/postgresql-42.2.18.jar
USER root
RUN chmod 777 /usr/local/spark/jars/postgresql-42.2.18.jar