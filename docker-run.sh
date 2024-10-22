# Build all docker containers required to run the service: postgres, redis and airflow
# For postgres and redis we use the original images defined in docker-compose.yml
# For airflow we build our own image based on the apache/airflow image (Dockerfile)
#    -> Note that line `build: .` in docker-compose.yaml
#
# Based on https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

export AIRFLOW_VERSION=2.9.2

setup=$1

# This contains access tokens and must be added manually
#source qa4sm.env

echo "Setting up DEVELOPMENT Setup, User = $setup"
export AIRFLOW_USER=$setup

#if [ `id -u` -ne 0 ]; then
#  echo "ERROR: Please run this script with sudo (for now)!"
#  exit 1
#fi

# These are required by docker compose and will be synchronized between this package and the container
mkdir -p ./dags ./logs ./plugins ./config

# The user ID passed to assign file permissions
echo -e "AIRFLOW_UID=$(id -u $AIRFLOW_USER)" > .env
# This is required by docker compose to mount the data dir as volume
# This is the path to ALL datasets that Airflow has access to!!
cat qa4sm.env >> .env
# Now we setup all the images that the workers might need
# Maybe pull them from dockerhub or geo.gitlab
# Maybe build them


# https://github.com/puckel/docker-airflow/issues/543
sudo chmod 777 /var/run/docker.sock

# start all necessary containers
docker compose up airflow-init
docker compose up
docker compose --profile flower up   # port 5555

if [ ! -e _airflow.sh ]
then
  curl -LfO 'https://airflow.apache.org/docs/apache-airflow/$AIRFLOW_VERSION/airflow.sh'
  mv airflow.sh _airflow.sh
fi

chmod +x _airflow.sh

