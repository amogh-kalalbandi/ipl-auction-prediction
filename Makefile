quality_checks:
	pylint --recursive=y .

shutdown:
	docker-compose down

build:
	docker-compose up -d --build

prefect_deploy: build
	sleep 40
	docker exec ipl-auction-prediction_prefect_server_1 prefect deploy -n ipl-auction-training
	docker exec ipl-auction-prediction_prefect_server_1 prefect deploy -n ipl-auction-prediction
	docker exec ipl-auction-prediction_prefect_server_1 prefect block register -m prefect_email
