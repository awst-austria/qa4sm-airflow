import os
import requests
import pandas as pd
import logging
from dotenv import load_dotenv


def load_qa4sm_dotenv():
    dotenv = os.path.join(os.path.dirname(__file__), '..', 'qa4sm.env')
    if os.path.isfile(dotenv):
        load_dotenv(dotenv)


def api_update_period(QA4SM_PORT_OR_NONE, QA4SM_IP_OR_URL, QA4SM_API_TOKEN,
                      ds_id, ti=None) -> str:
    new_ts_to_date: str = ti.xcom_pull("get_ts_timerange", key="ts_to")
    if QA4SM_PORT_OR_NONE.lower() not in ['none', '']:
        url = f"http://{QA4SM_IP_OR_URL}:{QA4SM_PORT_OR_NONE}/api/update-dataset-version"
    else:
        url = f"https://{QA4SM_IP_OR_URL}/api/update-dataset-version"

    headers = {
        "Authorization": f"Token {QA4SM_API_TOKEN}",
        "Content-Type": "application/json"
    }
    data = [
        {
            "id": str(ds_id),
            "time_range_end": str(new_ts_to_date)
        }
    ]
    response = requests.post(url, headers=headers, json=data)

    return str(response)


def decide_ts_update_required(ti=None) -> str:
    img_to = ti.xcom_pull(task_ids="get_img_timeranges", key="img_to")
    ts_to = ti.xcom_pull(task_ids="get_img_timeranges", key="ts_to")
    ts_next = ti.xcom_pull(task_ids="get_img_timeranges", key="ts_next")

    img_to = pd.to_datetime(img_to).to_pydatetime()
    ts_to = pd.to_datetime(ts_to).to_pydatetime() if ts_to is not None else None
    ts_next = pd.to_datetime(ts_next).to_pydatetime()

    logging.info(f"Image to: {img_to}")
    logging.info(f"Ts to: {ts_to}")
    logging.info(f"Ts Next to: {ts_next}")

    if ts_to is None:
        if img_to >= ts_next:
            continue_with = "extend_ts"
        else:
            continue_with = "get_ts_timerange"
    else:
        ts_to = pd.to_datetime(ts_to).to_pydatetime()
        if img_to > ts_to:
            continue_with = "extend_ts"
        else:
            continue_with = "get_ts_timerange"

    logging.info(f"Next task: {continue_with}")

    return continue_with