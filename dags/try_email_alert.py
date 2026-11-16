"""
Throwaway DAG to test failure-email alerting.

Trigger it manually in the UI. The single task logs the SMTP settings the
worker sees (without the password), checks that the SMTP host resolves, and
then fails on purpose. Airflow should then send a failure email to the
address(es) below. Check the task log for "Sent an alert email" (success) or
an SMTP traceback (failure).

Delete this DAG once alerting is confirmed to work.
"""
import logging
import os
import socket
from datetime import datetime

from airflow.configuration import conf
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)


def _check_smtp_and_fail():
    host = conf.get("smtp", "smtp_host")
    port = conf.getint("smtp", "smtp_port")
    logger.info(
        "SMTP config: host=%s port=%s starttls=%s ssl=%s user=%s mail_from=%s",
        host, port,
        conf.getboolean("smtp", "smtp_starttls"),
        conf.getboolean("smtp", "smtp_ssl"),
        conf.get("smtp", "smtp_user", fallback=None),
        conf.get("smtp", "smtp_mail_from"),
    )
    try:
        addrs = sorted({a[4][0] for a in socket.getaddrinfo(host, port)})
        logger.info("SMTP host %s resolves to %s", host, addrs)
    except socket.gaierror as e:
        logger.error("SMTP host %s does NOT resolve: %s", host, e)

    raise RuntimeError("Intentional failure to test the failure-email alert.")


with DAG(
        "Test-Email-Alert",
        default_args={
            "email": ["support@qa4sm.eu"],
            # Forced on, independent of EMAIL_ON_FAILURE, so the test always mails.
            "email_on_failure": True,
            "email_on_retry": False,
            "retries": 0,
        },
        description="Fails on purpose to test failure-email alerting",
        schedule=None,  # manual trigger only
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=["test", "email"],
) as dag:
    PythonOperator(
        task_id="fail_and_send_email",
        python_callable=_check_smtp_and_fail,
    )
