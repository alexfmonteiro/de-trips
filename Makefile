all: build start

default: start

build:
	docker-compose build

start:
	docker-compose up

reset:
	docker-compose down

run:
	docker exec spark spark-submit /home/jovyan/tests/test-spark.py
