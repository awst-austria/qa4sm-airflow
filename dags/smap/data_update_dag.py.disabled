"""
This is the processing pipeline to update SMAP L3 data in qa4sm.
It will download new image data from FTP, update time series in the qa4sm
datastore as far as image data is available and send the updated time series
period through the API to the DB.
"""
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator, Mount
from airflow.models.dag import DAG
from datetime import datetime, timedelta
import yaml
import os
import pandas as pd
import logging

from misc import (
    api_update_period, decide_ts_update_required,
    load_qa4sm_dotenv, api_update_fixtures, log_command
)

load_qa4sm_dotenv()
IMAGE = os.environ["SMAP_DAG_IMAGE"]
QA4SM_IP_OR_URL = os.environ["QA4SM_IP_OR_URL"]
QA4SM_PORT_OR_NONE = os.environ["QA4SM_PORT_OR_NONE"]
QA4SM_API_TOKEN = os.environ["QA4SM_API_TOKEN"]
QA4SM_DATA_PATH = os.environ["QA4SM_DATA_PATH"]  # On the HOST machine
EMAIL_ON_FAILURE = bool(int(os.environ.get("EMAIL_ON_FAILURE", 0)))

# Source is on the HOST machine (not airflow container), target is in the
# worker image
#   see also https://stackoverflow.com/questions/31381322/docker-in-docker
#   -cannot-mount-volume
data_mount = Mount(target="/qa4sm/data", source=QA4SM_DATA_PATH, type='bind')

logger = logging.getLogger(__name__)

"""
All versions are added to the list. The dag itself can be 
(de)activated in the GUI afterwards
"""
DAG_SETUP = {
    'SMAP_L3_V9': {
        # Paths here refer to the worker image and should not be changed!
        'img_path': "/qa4sm/data/SMAP_L3/SMAP_V9_AM_PM-ext/images",
        'ts_path': "/qa4sm/data/SMAP_L3/SMAP_V9_AM_PM-ext/timeseries",
        'ext_start_date': "2025-01-26",
        'qa4sm_dataset_id': "64",
    },
}


def get_timerange_from_yml(
        img_yml: str = None,
        ts_yml: str = None,
        ext_start_date: str = None,
        do_print: bool = False
) -> dict:
    if (img_yml is None) or (not os.path.isfile(img_yml)):
        logging.info(f"No img yml file: {img_yml}")
        img_to = None
    else:
        with open(img_yml, 'r') as stream:
            img_props = yaml.safe_load(stream)
        img_to = pd.to_datetime(img_props['last_day']).to_pydatetime()

    if (ts_yml is None) or (not os.path.isfile(ts_yml)):
        logging.info(f"No ts yml file: {ts_yml}")
        ts_to = None
    else:
        with open(ts_yml, 'r') as stream:
            ts_props = yaml.safe_load(stream)
        ts_to = pd.to_datetime(
            ts_props['last_day']).to_pydatetime()

    if ts_to is not None:
        ts_next = ts_to + timedelta(days=1)
    elif ext_start_date is not None:
        ts_next = pd.to_datetime(ext_start_date).to_pydatetime()
    else:
        ts_next = None

    logging.info(f"Images time to: {img_to}")
    logging.info(f"Ts time to: {ts_to}")
    logging.info(f"Ts next: {ts_next}")

    if do_print:
        print("Images time to: ", img_to)
        print("TS time to: ", ts_to)
        print("Ts next: ", ts_next)

    return {'img_to': str(img_to.date() if img_to is not None else None),
            'ts_to': str(ts_to.date()) if ts_to is not None else None,
            'ts_next': str(ts_next.date())}


for version, dag_settings in DAG_SETUP.items():
    img_path = dag_settings['img_path']
    ts_path = dag_settings['ts_path']
    ext_start_date = dag_settings['ext_start_date']
    qa4sm_id = dag_settings['qa4sm_dataset_id']

    ts_yml_file = os.path.join(ts_path, 'overview.yml')
    img_yml_file = os.path.join(img_path, 'overview.yml')

    with (DAG(
            f"SMAP_L3-{version}-Processing",
            default_args={
                "depends_on_past": False,
                "email": ["support@qa4sm.eu"],
                "email_on_failure": EMAIL_ON_FAILURE,
                "email_on_retry": False,
                "retries": 1,
                "retry_delay": timedelta(hours=1),
            },
            description="Update SMAP image data",
            schedule=timedelta(weeks=1),
            start_date=datetime(2025, 12, 14),
            catchup=False,  # avoid duplicate processing
            tags=["smap_l3", "download", "reshuffle", "update"],
    ) as dag):
        # Check data setup
        # -----------------------------------------------------
        _task_id = "verify_dir_available"
        _doc = f"""
        Check if the data store is mounted, check if {img_path} and 
{ts_path} exist.
        """
        _command = f"bash -c '[ -d \"{img_path}\" ] && [ -d \"{ts_path}\" ] || exit 1'"
        logging.info(f"Running Container Command in {IMAGE}: {_command}")
        # start container with sudo?
        verify_dir_available = DockerOperator(
            task_id=_task_id,
            image=IMAGE,
            container_name=f'task__smapl3__{version}__{_task_id}',
            privileged=True,
            command=_command,
            mounts=[data_mount],
            force_pull=False,
            # make sure the image is pulled once the start of the pipeline
            auto_remove="force",
            mount_tmp_dir=False,
            docker_url='tcp://docker-proxy:2375',
            on_execute_callback=log_command,
            doc=_doc
        )

        _task_id = "verify_qa4sm_reachable"
        _command = (f"(set -e; "
                    f"nc -z -v {QA4SM_IP_OR_URL} "
                    f"{QA4SM_PORT_OR_NONE if QA4SM_PORT_OR_NONE.lower() not in ['none', ''] else 443} 2>&1 | grep -q 'succeeded' && echo 'OK') || exit 1")
        _doc = f"""
        Check if qa4sm is reachable, 0 = success, 1 = fail
        """
        logging.info(f"Running Container Command in {IMAGE}: {_command}")
        # start container with sudo?
        verify_qa4sm_available = BashOperator(
            task_id=_task_id,
            bash_command=_command,
            on_execute_callback=log_command,
            doc=_doc
        )

        # CDS Image
        # Download----------------------------------------------------
        _task_id = f'update_images'
        _doc = f"""
        This task will check if any image data is available in the img_path.
        If not it downloads all available data after the ext_start_date (first
        time only). Otherwise, it will download new data on the server, after
        the data that is already available.
        This will not replace any existing files locally. This finds the 
        LATEST 
        local file and checks if any new data AFTER this date is available.
        """
        # TODO Check how to access smap download
        _command = f"""bash -c '[ "$(ls -A {img_path})" ] && smap_l3 update_img --output {img_path} --username {os.environ["SMAP_USERNAME"]} --password {os.environ["SMAP_PASSWORD"]}||  smap_l3 download --output {img_path} --time_start {ext_start_date} --username {os.environ["SMAP_USERNAME"]} --password {os.environ["SMAP_PASSWORD"]} --version 009 '"""
        logging.info(f"Running Container Command in {IMAGE}: {_command}")

        # https://airflow.apache.org/docs/apache-airflow-providers-docker
        # /stable/_api/airflow/providers/docker/operators/docker/index.html
        # TODO check if name arbitrary
        update_images = DockerOperator(
            task_id=_task_id,
            image=IMAGE,
            container_name=f'task__smap__{version}__{_task_id}',
            privileged=True,
            command=_command,
            mounts=[data_mount],
            auto_remove="force",
            timeout=3600 * 2,
            mount_tmp_dir=False,
            docker_url='tcp://docker-proxy:2375',
            on_execute_callback=log_command,
            doc=_doc,
        )

        # Get current time series coverage
        # -------------------------------------
        _task_id = "get_img_timeranges"
        _doc = f"""
        Look at time series and image data and infer their covered period.
        """
        get_img_timeranges = PythonOperator(
            task_id=_task_id,
            python_callable=get_timerange_from_yml,
            op_kwargs={'img_yml': img_yml_file,
                       'ts_yml': ts_yml_file,
                       'ext_start_date': ext_start_date,
                       'do_print': True},
            multiple_outputs=True,
            do_xcom_push=True,
            on_execute_callback=log_command,
            doc=_doc,
        )

        # Update time series?
        # --------------------------------------------------
        _task_id = "decide_ts_update"
        _doc = f"""
        Compare image period and time series period and decide if update 
        needed.
        This can result in
        - 'finish': No processing required
        - 'update_ts': If extensions exist, update them
        """
        decide_reshuffle = BranchPythonOperator(
            task_id=_task_id,
            python_callable=decide_ts_update_required,
            on_execute_callback=log_command,
            doc=_doc,
        )

        # TODO test
        # Optional: Initial extension TS
        # ---------------------------------------
        _task_id = "extend_ts"
        # VARS = "soil_moisture,soil_moisture_error,retrieval_qual_flag,freeze_thaw_fraction,surface_flag,surface_temperature,vegetation_opacity,vegetation_water_content,landcover_class,static_water_body_fraction,tb_time_seconds"
        # _command = f"""bash -c '[ "$(ls -A {os.path.join(ts_path, '*.nc')})] && smap_l3 update_ts {ts_path} {img_path} || smap_l3 reshuffle --overpass BOTH --var_overpass_str True --time_key tb_time_seconds {img_path} {ts_path}  {VARS} '"""
        # VARS = "soil_moisture,soil_moisture_error,retrieval_qual_flag,freeze_thaw_fraction,surface_flag,surface_temperature,vegetation_opacity,vegetation_water_content,landcover_class,static_water_body_fraction,tb_time_seconds"
        #
        # _command = f"""bash -c "[ \"$(ls -A {os.path.join(ts_path, '*.nc')})\" ] && smap_l3 update_ts {ts_path} {img_path} || smap_l3 reshuffle --overpass BOTH --var_overpass_str True --time_key tb_time_seconds {img_path} {ts_path} '{VARS}' " """
        VARS = "soil_moisture,soil_moisture_error,retrieval_qual_flag,freeze_thaw_fraction,surface_flag,surface_temperature,vegetation_opacity,vegetation_water_content,landcover_class,static_water_body_fraction,tb_time_seconds"

        _command = f"""bash -c '[ "$(ls -A {os.path.join(ts_path, '*.nc')})" ] && smap_l3 update_ts {ts_path} {img_path} || """ \
                f"""smap_l3 reshuffle --overpass BOTH --var_overpass_str True --time_key tb_time_seconds {img_path} {ts_path} {VARS}' """

        _doc = f"""
        Creates new time series, or appends new data in time series format.
        """
        logging.info(f"Running Container Command in {IMAGE}: {_command}")
        extend_ts = DockerOperator(
            task_id=_task_id,
            image=IMAGE,
            container_name=f'task__smap_l3__{version}__{_task_id}',
            privileged=True,
            command=_command,
            mounts=[data_mount],
            auto_remove="force",
            timeout=3600 * 2,
            mount_tmp_dir=False,
            docker_url='tcp://docker-proxy:2375',
            on_execute_callback=log_command,
            doc=_doc,
        )

        # Optional: Get updated time range
        # -------------------------------------
        _task_id = "get_ts_timerange"
        _doc = f"""
        Get the current temporal coverage of the time series data
        """
        get_ts_timerange = PythonOperator(
            task_id=_task_id,
            python_callable=get_timerange_from_yml,
            op_kwargs={'img_yml': None, 'ts_yml': ts_yml_file,
                       'ext_start_date': None, 'do_print': False},
            multiple_outputs=True,
            trigger_rule="none_failed_min_one_success",
            do_xcom_push=True,
            on_execute_callback=log_command,
            doc=_doc,
        )

        # Optional: Send new time range to QA4SM
        # -------------------------------
        _doc = f"""
        Report the latest covered period to the service to update it on the 
        website.
        """
        update_period = PythonOperator(
            task_id='api_update_period',
            python_callable=api_update_period,
            op_kwargs={'QA4SM_PORT_OR_NONE': QA4SM_PORT_OR_NONE,
                       'QA4SM_IP_OR_URL': QA4SM_IP_OR_URL,
                       'QA4SM_API_TOKEN': QA4SM_API_TOKEN,
                       'ds_id': qa4sm_id},
            on_execute_callback=log_command,
            doc=_doc,
        )

        # Optional: Update and push fixtures
        # -----------------------------------
        _task_id = "api_update_fixtures"
        _doc = f"""
        Dump the updated periods to fixtures and push to github.
        """
        update_fixtures = PythonOperator(
            task_id=_task_id,
            python_callable=api_update_fixtures,
            op_kwargs={'QA4SM_PORT_OR_NONE': QA4SM_PORT_OR_NONE,
                       'QA4SM_IP_OR_URL': QA4SM_IP_OR_URL,
                       'QA4SM_API_TOKEN': QA4SM_API_TOKEN
                       },
            on_execute_callback=log_command,
            doc=_doc,
        )

        # Always check the current time range again
        # ----------------------------
        _task_id = "finish"
        _doc = f""" 
        Print the current coverages again. This is just a wrap-up task that 
        ALWAYS runs.
        """
        finish = PythonOperator(
            task_id=_task_id,
            python_callable=get_timerange_from_yml,
            op_kwargs={'img_yml': img_yml_file,
                       'ts_yml': ts_yml_file,
                       'ext_start_date': ext_start_date,
                       'do_print': True},
            on_execute_callback=log_command,
            doc=_doc,
        )

        # Task logic
        # -----------------------------------------------------------
        verify_dir_available >> verify_qa4sm_available >> update_images >> get_img_timeranges >> decide_reshuffle
        decide_reshuffle >> extend_ts >> get_ts_timerange >> update_period >> update_fixtures >> finish
        decide_reshuffle >> get_ts_timerange >> update_period >> update_fixtures >> finish
