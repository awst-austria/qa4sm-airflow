"""
This is the processing pipeline to update SMOS L2 data in qa4sm.
It will download new image data from FTP, update time series in the qa4sm
datastore as far as image data is available and send the updated time series
period through the API to the DB.
"""
from airflow.operators.python import (
    PythonOperator,
    BranchPythonOperator)
from airflow.sensors.python import PythonSensor
from airflow.models.dag import DAG
from datetime import datetime, timedelta
from pathlib import Path
import logging
import shutil
import os.path
from datetime import date
from dateutil.relativedelta import relativedelta
import calendar

from qa4sm_apps import Connection
from qa4sm_apps.autoreports.series import AutoReportSeries
from qa4sm_apps.autoreports.utils import ValidationReportError

QA4SM_IP_OR_URL = os.environ["QA4SM_IP_OR_URL"]
QA4SM_PORT_OR_NONE = os.environ["QA4SM_PORT_OR_NONE"]

if QA4SM_PORT_OR_NONE.lower() in ["none", ""]:
    QA4SM_INSTANCE = QA4SM_IP_OR_URL
else:
    QA4SM_INSTANCE = f"{QA4SM_IP_OR_URL}:{QA4SM_PORT_OR_NONE}"

QA4SM_REPORTS_PATH = Path(os.environ["QA4SM_REPORTS_PATH"])  # On the HOST machine
QA4SM_TOKEN = os.environ["QA4SM_API_TOKEN"]
EMAIL_ON_FAILURE = bool(int(os.environ.get("EMAIL_ON_FAILURE", 0)))

connection = Connection(QA4SM_INSTANCE, token=QA4SM_TOKEN, quiet_login=False)

report_collection = QA4SM_REPORTS_PATH / "pdf_reports" / "SMOS_L2_v700"
series_root = QA4SM_REPORTS_PATH / "results" / "SMOS_L2_v700"
config_path = "/home/wpreimes/shares/home/code/qa4sm-apps/configs/smos_l2_v700/report_config_templates"
latex_templ_path = "/home/wpreimes/shares/home/code/qa4sm-apps/configs/smos_l2_v700/report_latex_templates/src"

assert os.path.exists(report_collection)
assert os.path.exists(series_root)

logger = logging.getLogger(__name__)


def _get_report_name(**context) -> tuple[str, str, str]:
    """
    Derive report_name, interval_from and interval_to from the logical_date
    of the current DAG run.
    """
    report_date = context["logical_date"].date().replace(day=1)
    interval_from, interval_to = period_for_report(str(report_date), period_months=3)
    report_name = f"{interval_from}_to_{interval_to}"
    return report_name, interval_from, interval_to


def _verify_service_access(connection, **context):
    """
    Verify that airflow can log in as a user.

    Raises
    ------
    ValidationReportError: When the login was not successful
    """
    user = connection.session.user
    print(f"Airflow API connection successful. User: {user}.")
    # FIX #6: was `!=`, should be `==` — raise when NOT authenticated
    if user.lower() == "anonymous":
        raise ValidationReportError("Service is not accessible.")


def period_for_report(ref_date: str, period_months: int = 3) -> tuple[str, str]:
    """
    Take the reference date and compute the interval for the validation
    report, ie. the start_date is the beginning of the month 3 (default)
    months before the reference date. And the end is the end of the month
    before the reference date. e.g. 2025-02-07 -> (2024-11-01), (2025-01-31)

    Parameters
    ----------
    ref_date : str
        Reference date (ISO format: YYYY-MM-DD) from which the period is
        subtracted.
    period_months : int, optional
        Number of months to go back from the reference date to determine
        the start of the interval. Defaults to 3.

    Returns
    -------
    interval_from : str
        Start date of the chosen period interval (first day of the month
        ``period_months`` before ``ref_date``), in ISO format YYYY-MM-DD.
    interval_to : str
        End date of the chosen period interval (last day of the month
        preceding ``ref_date``), in ISO format YYYY-MM-DD.

    Examples
    --------
    >>> period_for_report("2025-02-07")
    ('2024-11-01', '2025-01-31')
    >>> period_for_report("2025-03-15")
    ('2024-12-01', '2025-02-28')
    >>> period_for_report("2024-03-15")  # leap year
    ('2023-12-01', '2024-02-29')
    >>> period_for_report("2025-02-07", period_months=6)
    ('2024-08-01', '2025-01-31')
    """
    ref = date.fromisoformat(ref_date)

    # Start: first day of the month `period_months` before ref_date
    start = ref.replace(day=1) - relativedelta(months=period_months)

    # End: last day of the month before ref_date
    end_month = ref.replace(day=1) - relativedelta(months=1)
    end = end_month.replace(day=calendar.monthrange(end_month.year, end_month.month)[1])

    return start.isoformat(), end.isoformat()


def _is_staging_required_branch(**context) -> str:
    """
    If a report for a period was already staged, we don't have to
    stage it again, and can directly check if we have to process the
    validation run.
    """
    report_name, _, _ = _get_report_name(**context)
    series = AutoReportSeries(series_root=series_root, connection=connection)

    dir_exists = (series.series_root / report_name).exists()
    report_in_series = report_name in series.reports.keys()

    if (not dir_exists) and (not report_in_series):
        return 'stage_new_report'
    else:
        return 'is_processing_required_branch'


def _stage_new_report(**context):
    report_name, interval_from, interval_to = _get_report_name(**context)
    series = AutoReportSeries(series_root=series_root, connection=connection)

    series.new_report(
        report_name, config_path,
        override_params={                   # FIX #4: was `override_kwargs`
            'interval_from': interval_from,
            'interval_to': interval_to,
            # todo: delete:
            "min_lat": -17.0,
            "min_lon": 140.0,
            "max_lat": -12.0,
            "max_lon": 150.0,
        },
        instance=QA4SM_INSTANCE,
        token=QA4SM_TOKEN,
    )


def _is_processing_required_branch(**context) -> str:
    """
    Check if report was not already triggered or processed before:
        - 0 - Staged: Local setup created, not triggered online
        - 1 - Started: All runs were triggered
        - 2 - Processed: All runs have finished online
        - 3 - Collected: All results were downloaded locally
        - 4 - Compiled: PDF was created
    """
    report_name, _, _ = _get_report_name(**context)
    series = AutoReportSeries(series_root=series_root, connection=connection)

    if series[report_name].status == 0:
        return 'check_data_availability'
    else:
        return 'is_compiling_required_branch'


def _is_compiling_required_branch(**context) -> str:
    """
    Check if report was not already compiled:
        - 0 - Staged: Local setup created, not triggered online
        - 1 - Started: All runs were triggered
        - 2 - Processed: All runs have finished online
        - 3 - Collected: All results were downloaded locally
        - 4 - Compiled: PDF was created
    """
    report_name, _, _ = _get_report_name(**context)
    series = AutoReportSeries(series_root=series_root, connection=connection)

    if series[report_name].status == 2:
        return 'collect_and_compile'
    else:
        return 'finish'


def _start_validation_runs(**context):
    """
    The data is available and the runs have not been processed before.
    Trigger the runs in the report. The settings were overridden before when
    staging the report already, so we don't have to override them again.
    The validation runs process asynchronously, so this function will return
    immediately.
    """
    report_name, _, _ = _get_report_name(**context)
    series = AutoReportSeries(series_root=series_root, connection=connection)  # FIX #3: was `connection`
    series[report_name].start_all_runs()


def _sense_runs_finished(**context) -> bool:
    """
    Verify if the report status is "processed":
        - 0 - Staged: Local setup created, not triggered online
        - 1 - Started: All runs were triggered
        - 2 - Processed: All runs have finished online
        - 3 - Collected: All results were downloaded locally
        - 4 - Compiled: PDF was created
    """
    report_name, _, _ = _get_report_name(**context)
    series = AutoReportSeries(series_root=series_root, connection=connection)
    return series[report_name].status >= 2


def _sense_data_available(**context) -> bool:
    """
    Verify whether the required datasets are already in the service.
    """
    report_name, _, _ = _get_report_name(**context)
    series = AutoReportSeries(series_root=series_root, connection=connection)
    return series[report_name].verify_dataset_availability()


def _collect_and_compile(**context):
    report_name, _, _ = _get_report_name(**context)
    series = AutoReportSeries(series_root=series_root, connection=connection)  # FIX #3: was `connection`

    series[report_name].collect_content()

    series.track_metric(metric='urmsd_between_0-SMOS_L2_and_1-C3S_combined',
                        unit='m³m⁻³', ref_epoch=report_name, n_epochs=12,
                        path_out=series[report_name].report_root)

    series.track_metric(metric='urmsd_between_0-SMOS_L2_and_1-ERA5_LAND',
                        unit='m³m⁻³', ref_epoch=report_name, n_epochs=12,
                        path_out=series[report_name].report_root)

    series.track_metric(metric='R_between_0-SMOS_L2_and_1-C3S_combined',
                        pretty_name='R', unit='-', ref_epoch=report_name,
                        n_epochs=12,
                        p_mask_var='p_R_between_0-SMOS_L2_and_1-C3S_combined',
                        path_out=series[report_name].report_root)

    series.track_metric(metric='R_between_0-SMOS_L2_and_1-ERA5_LAND',
                        pretty_name='R', unit='-', ref_epoch=report_name,
                        n_epochs=12,
                        p_mask_var='p_R_between_0-SMOS_L2_and_1-ERA5_LAND',
                        path_out=series[report_name].report_root)

    series[report_name].compile(template_path=latex_templ_path, tex_ignore=None)

    shutil.copy(
        series[report_name].report_root / 'pdf_report' / 'main.pdf',
        report_collection / f"{report_name}.pdf"
    )


# The dag runs every month and tries to build the validation report for the
# previous month. It will wait up to 90 days for data to become available.
with DAG(
        "SMOS_L2-v700-Autoreport",
        default_args={
            "depends_on_past": True,
            # FIX #5: removed `catchup` from default_args, it's a DAG-level arg
            "email": ["support@qa4sm.eu"],
            "email_on_failure": EMAIL_ON_FAILURE,
            "email_on_retry": EMAIL_ON_FAILURE,
            "retries": 3,
            "retry_delay": timedelta(hours=3),
        },
        description="Create SMOS L2 validation report",
        schedule="0 0 1 * *",  # 1st of each month
        start_date=datetime(2024, 1, 1),
        end_date=datetime(2024, 12, 31),   # todo: delete
        catchup=True,
        max_active_runs=1,
        tags=["smos_l2", "v700", "autoreport"],
) as dag:

    verify_service_access = PythonOperator(
        task_id="verify_service_access",
        python_callable=_verify_service_access,
        op_kwargs={"connection": connection},
        doc="Establish a connection to the instance from the global config "
            "using the token from the config. Will fail if the login as user "
            "does not work."
    )

    is_staging_required_branch = BranchPythonOperator(
        task_id="is_staging_required_branch",
        python_callable=_is_staging_required_branch,
        doc="For this run, check if a validation report for the respective "
            "reference date already exists or if it should be staged in the "
            "next step."
    )

    stage_new_report = PythonOperator(
        task_id="stage_new_report",
        python_callable=_stage_new_report,
        doc="Stage a new report. This will only create the report directory "
            "and place the run config files. No processing happens yet. Data "
            "availability is NOT yet required to stage a report. After staging "
            "we can use the function to check if the data is available already "
            "and if the processing was already triggered or not."
    )

    is_processing_required_branch = BranchPythonOperator(
        task_id="is_processing_required_branch",
        python_callable=_is_processing_required_branch,
        doc="The report was staged, but the results might not be processed yet. "
            "Check if processing is required (to trigger the validation runs "
            "afterwards), or if they were already triggered (to check if the "
            "data collection is required)."
    )

    wait_for_data = PythonSensor(
        task_id="wait_for_data",
        python_callable=_sense_data_available,
        poke_interval=60 * 60 * 24,        # check every 24 hours
        timeout=60 * 60 * 24 * 90,         # FIX #7: was * 60 (60 days), now * 90 (90 days)
        mode="reschedule",                 # frees up worker slot while waiting
        doc="Wait for the data to be available in the service. "
            "This can take up to 12 weeks; we check once per day and continue "
            "to trigger processing once data is available."
    )

    start_validation_runs = PythonOperator(
        task_id="start_validation_runs",
        python_callable=_start_validation_runs,
        doc="Data is available in the service. The report was staged with the "
            "correct settings. Trigger all validation runs asynchronously "
            "(returns immediately) and wait for completion in the next step."
    )

    wait_for_validation = PythonSensor(
        task_id="wait_for_validation",     # FIX #2: was duplicate "wait_for_data"
        python_callable=_sense_runs_finished,
        poke_interval=60 * 60 * 1,         # check every hour
        timeout=60 * 60 * 24 * 5,          # give up after 5 days
        mode="reschedule",                 # frees up worker slot while waiting
        doc="Wait for the validation runs triggered on the server to finish. "
            "Normally takes a few hours, but large runs (e.g. SMOS vs ERA5-Land) "
            "may take a few days."
    )

    is_compiling_required_branch = BranchPythonOperator(
        task_id="is_compiling_required_branch",
        python_callable=_is_compiling_required_branch,
        doc="The validation runs are done. Check if the report still needs to "
            "be compiled or was already compiled in a previous attempt."
    )

    collect_and_compile = PythonOperator(
        task_id="collect_and_compile",
        python_callable=_collect_and_compile,
        doc="All validations have finished. Download the results and compile "
            "the PDF report."
    )

    finish = PythonOperator(
        task_id="finish",
        python_callable=lambda **context: print("Finish"),
        doc="No-op terminal task to close the DAG run cleanly.",
    )

    verify_service_access >> is_staging_required_branch
    is_staging_required_branch >> stage_new_report >> is_processing_required_branch
    is_staging_required_branch >> is_processing_required_branch
    is_processing_required_branch >> wait_for_data >> start_validation_runs >> wait_for_validation >> is_compiling_required_branch
    is_processing_required_branch >> is_compiling_required_branch
    is_compiling_required_branch >> collect_and_compile >> finish
    is_compiling_required_branch >> finish