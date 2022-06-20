FROM  docker.io/jupyter/pyspark-notebook@sha256:d9c1167f5ea7b5944ac9c03eb65ed12812841cf9c50b8ca0d22905edd401407c

RUN pip3 install psycopg2-binary geopandas
ADD https://jdbc.postgresql.org/download/postgresql-42.2.18.jar /usr/local/spark/jars/postgresql-42.2.18.jar
USER root
RUN chmod 777 /usr/local/spark/jars/postgresql-42.2.18.jar