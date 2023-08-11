quality_checks:
	pylint --recursive=y .

shutdown:
	docker-compose down

build:
	docker-compose up -d --build

prefect_deploy: build
	sleep 30
	docker exec ipl-auction-prediction_prefect_server_1 prefect deploy -n ipl-auction-training
	docker exec ipl-auction-prediction_prefect_server_1 prefect deploy -n ipl-auction-prediction
