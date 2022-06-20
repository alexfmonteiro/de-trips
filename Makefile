default: start

build:
	docker-compose build

start:
	docker-compose up --build

reset:
	docker-compose down

run:
	docker exec spark spark-submit ./jobs/trips_etl.py
