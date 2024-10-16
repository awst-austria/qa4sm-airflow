# Remove the airflow docker container and images (keep redis and postgres)

docker compose down

docker image rm docker-airflow-worker
docker image rm docker-airflow-webserver
docker image rm docker-airflow-triggerer
docker image rm docker-airflow-scheduler
docker image rm docker-airflow-init

#sudo chown -R $(whoami) .