# Remove the airflow docker container and images (keep redis and postgres)

docker compose down

docker image rm qa4sm-airflow-airflow-worker
docker image rm qa4sm-airflow-airflow-webserver
docker image rm qa4sm-airflow-airflow-triggerer
docker image rm qa4sm-airflow-airflow-scheduler
docker image rm qa4sm-airflow-airflow-init
docker image rm qa4sm-airflow-flower

#sudo chown -R $(whoami) .
