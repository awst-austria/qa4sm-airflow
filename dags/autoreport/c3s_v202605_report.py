"""
Quarterly automated validation report for C3S Soil Moisture v202605
(COMBINED, ACTIVE and PASSIVE).

Each DAG run creates the report for the calendar quarter that ended before
the run (e.g. the run on 2025-04-01 creates the report for 2025-01-01 to
2025-03-31). The report is staged with the config templates in
c3s_v202605_quarterly/report_config_templates, the validation runs are
triggered once the data is available in QA4SM, and the downloaded results
are compiled into a PDF using the templates in
c3s_v202605_quarterly/report_latex_templates/src.

Runs (order of the config templates):
    Run1: C3S COMBINED, ACTIVE, PASSIVE vs. ISMN (main validation)
    Run2: C3S COMBINED, ACTIVE, PASSIVE vs. ERA5-Land (short chapter,
          global tracking of R / ubRMSD and spatial coverage)
"""
from airflow.providers.standard.operators.python import (
    PythonOperator,
    BranchPythonOperator)
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.timetables.interval import CronDataIntervalTimetable

from datetime import datetime, timedelta, date
from pathlib import Path
import logging
import shutil
import os.path
from dateutil.relativedelta import relativedelta
import calendar
from misc import load_qa4sm_dotenv
from qa4sm_autoreports.data import Data

load_qa4sm_dotenv()

QA4SM_IP_OR_URL = os.environ["QA4SM_IP_OR_URL"]
QA4SM_PORT_OR_NONE = os.environ["QA4SM_PORT_OR_NONE"]

if QA4SM_PORT_OR_NONE.lower() in ["none", ""]:
    QA4SM_INSTANCE = QA4SM_IP_OR_URL
else:
    QA4SM_INSTANCE = f"{QA4SM_IP_OR_URL}:{QA4SM_PORT_OR_NONE}"

QA4SM_REPORTS_ROOT = Path(os.environ["QA4SM_REPORTS_PATH"])  # On the HOST machine
QA4SM_TOKEN = os.environ["QA4SM_API_TOKEN"]
EMAIL_ON_FAILURE = bool(int(os.environ.get("EMAIL_ON_FAILURE", 0)))

SERIES_NAME = "C3S_v202605_Quarterly"
RUN_PREFIX = "C3S v202605"  # run folder names, see main.tex (\import)
N_RUNS = 2

report_collection = QA4SM_REPORTS_ROOT / "pdf" / SERIES_NAME
series_root = QA4SM_REPORTS_ROOT / "results" / SERIES_NAME
dag_dir = Path(__file__).parent
config_path = dag_dir / "c3s_v202605_quarterly" / "report_config_templates"
latex_templ_path = dag_dir / "c3s_v202605_quarterly" / "report_latex_templates" / "src"

assert os.path.exists(report_collection), f"Path not found {report_collection}"
assert os.path.exists(series_root), f"Path not found {series_root}"
assert os.path.exists(config_path), f"Path not found {config_path}"
assert os.path.exists(latex_templ_path), f"Path not found {latex_templ_path}"

logger = logging.getLogger(__name__)


def quarter_for_report(ref_date: str) -> tuple[str, str]:
    """
    Last complete calendar quarter before the reference date.

    Parameters
    ----------
    ref_date : str
        Reference date (ISO format: YYYY-MM-DD).

    Returns
    -------
    interval_from : str
        First day of the quarter, YYYY-MM-DD.
    interval_to : str
        Last day of the quarter, YYYY-MM-DD.

    Examples
    --------
    >>> quarter_for_report("2025-04-01")
    ('2025-01-01', '2025-03-31')
    >>> quarter_for_report("2025-05-17")
    ('2025-01-01', '2025-03-31')
    >>> quarter_for_report("2025-01-01")
    ('2024-10-01', '2024-12-31')
    """
    ref = date.fromisoformat(ref_date)
    # first month of the quarter containing ref_date
    q_start = ref.replace(month=3 * ((ref.month - 1) // 3) + 1, day=1)
    start = q_start - relativedelta(months=3)
    end_month = q_start - relativedelta(months=1)
    end = end_month.replace(
        day=calendar.monthrange(end_month.year, end_month.month)[1])
    return start.isoformat(), end.isoformat()


def _get_report_name(**context) -> tuple[str, str, str]:
    """
    Derive report_name, interval_from and interval_to of the current DAG run.
    The report covers the last complete quarter before the end of the data
    interval of the run (i.e. the trigger date of a scheduled run).
    """
    dag_run = context["dag_run"]
    # Scheduled runs: data_interval_end = trigger date (1st of Jan/Apr/Jul/Oct).
    # Manual runs in Airflow 3 may have no data interval -> use run_after.
    ref = context.get("data_interval_end") or dag_run.run_after
    interval_from, interval_to = quarter_for_report(str(ref.date()))
    report_name = f"{interval_from}_to_{interval_to}"
    return report_name, interval_from, interval_to


def _connect_series():
    from qa4sm_autoreports import Connection
    from qa4sm_autoreports.series import AutoReportSeries

    connection = Connection(QA4SM_INSTANCE, token=QA4SM_TOKEN, quiet_login=True)
    return AutoReportSeries(series_root=series_root, connection=connection)


def _verify_service_access(**context):
    """
    Verify that airflow can log in as a user.

    Raises
    ------
    ValidationReportError: When the login was not successful
    """
    from qa4sm_autoreports import Connection
    from qa4sm_autoreports.utils import ValidationReportError

    logger.info("Verifying service access to %s", QA4SM_INSTANCE)
    connection = Connection(QA4SM_INSTANCE, token=QA4SM_TOKEN, quiet_login=True)
    user = connection.session.user
    logger.info("API connection successful. Logged in as: %s", user)
    if user.lower() == "anonymous":
        logger.error("Login returned anonymous user — service is not accessible")
        raise ValidationReportError("Service is not accessible.")


def _is_staging_required_branch(**context) -> str:
    """
    If a report for a period was already staged, we don't have to
    stage it again, and can directly check if we have to process the
    validation run.
    """
    report_name, _, _ = _get_report_name(**context)
    logger.info("[%s] Checking if staging is required", report_name)
    series = _connect_series()

    dir_exists = (series.series_root / report_name).exists()
    report_in_series = report_name in series.reports.keys()
    logger.info("[%s] dir_exists=%s, report_in_series=%s",
                report_name, dir_exists, report_in_series)

    if (not dir_exists) and (not report_in_series):
        logger.info("[%s] → branching to: stage_new_report", report_name)
        return 'stage_new_report'
    else:
        logger.info("[%s] → branching to: is_processing_required_branch", report_name)
        return 'is_processing_required_branch'


def _stage_new_report(**context):
    report_name, interval_from, interval_to = _get_report_name(**context)
    logger.info("[%s] Staging new report (interval: %s → %s)",
                report_name, interval_from, interval_to)
    series = _connect_series()

    report = series.new_report(
        report_name, config_path,
        override_params={
            'interval_from': interval_from,
            'interval_to': interval_to,
            # global (the bounding box in the config templates is empty)
            'min_lat': -90.0,
            'min_lon': -180.0,
            'max_lat': 90.0,
            'max_lon': 180.0,
        },
        instance=QA4SM_INSTANCE,
        token=QA4SM_TOKEN,
    )

    # Run order follows the config template names (01_*, 02_*)
    report.restructure_runs({f'run{i}': f"{RUN_PREFIX} {report_name} Run{i}"
                             for i in range(1, N_RUNS + 1)})

    logger.info("[%s] Staging complete", report_name)


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
    logger.info("[%s] Checking processing status", report_name)
    series = _connect_series()

    status = series[report_name].status
    logger.info("[%s] Current status: %d", report_name, status)

    if status == 0:
        logger.info("[%s] → branching to: wait_for_data", report_name)
        return 'wait_for_data'
    elif status == 1:
        logger.info("[%s] → branching to: wait_for_validation", report_name)
        return 'wait_for_validation'
    else:
        logger.info("[%s] → branching to: is_compiling_required_branch", report_name)
        return 'is_compiling_required_branch'


def _is_compiling_required_branch(**context) -> str:
    """
    Check if report was not already compiled (status 4).
    """
    from qa4sm_autoreports.utils import ValidationReportError

    report_name, _, _ = _get_report_name(**context)
    logger.info("[%s] Checking if compiling is required", report_name)
    series = _connect_series()

    status = series[report_name].status
    logger.info("[%s] Current status: %d", report_name, status)

    if status in [2, 3]:
        logger.info("[%s] → branching to: collect_and_compile", report_name)
        return 'collect_and_compile'
    elif status == 4:
        logger.info("[%s] → branching to: finish (already compiled)", report_name)
        return 'finish'
    else:
        raise ValidationReportError(
            f"[{report_name}] Unexpected status {status} in "
            f"is_compiling_required_branch; validations not finished, "
            f"cannot skip to finish."
        )


def _start_validation_runs(**context):
    """
    The data is available and the runs have not been processed before.
    Trigger the runs in the report (asynchronously, returns immediately).
    """
    report_name, _, _ = _get_report_name(**context)
    logger.info("[%s] Starting all validation runs", report_name)
    series = _connect_series()
    series[report_name].start_all_runs()
    logger.info("[%s] All validation runs triggered (async)", report_name)


def _sense_runs_finished(**context) -> bool:
    """
    Verify if the report status is at least "processed" (2).
    """
    report_name, _, _ = _get_report_name(**context)
    series = _connect_series()
    status = series[report_name].status
    finished = status >= 2
    logger.info("[%s] Validation runs finished check: status=%d, finished=%s",
                report_name, status, finished)
    return finished


def _sense_data_available(**context) -> bool:
    """
    Verify whether the required datasets are already in the service.
    """
    report_name, _, _ = _get_report_name(**context)
    series = _connect_series()
    is_available = series[report_name].verify_dataset_availability()

    for i, run in enumerate(series[report_name].runs.values()):
        logger.info("Run %d: Start=%s, End=%s", i, run.config['interval_from'],
                    run.config['interval_to'])

    logger.info("[%s] Data availability check: available=%s",
                report_name, is_available)
    return is_available


def _traffic_light(tracking_status: list) -> str:
    s = [v for v in tracking_status if v != "none"]
    if not s:
        return "none"
    elif all(v == "ok" for v in s):
        return "green"
    elif all(v == "not ok" for v in s):
        return "red"
    else:
        return "yellow"


def _collect_and_compile(**context):
    n_epochs = 15
    report_name, _, interval_to = _get_report_name(**context)
    logger.info("[%s] Starting collect and compile", report_name)
    series = _connect_series()
    report_root = series[report_name].report_root

    logger.info("[%s] Collecting results", report_name)
    series[report_name].collect_content()

    products = ['1-C3S_combined', '2-C3S_active', '3-C3S_passive']

    # Metrics across all locations, expected range from the previous reports:
    # (metric, unit, p-value variable for masking, alpha)
    # - ISMN: Pearson's R (the ubRMSD is tracked per land cover class below)
    # - ERA5-Land: Pearson's R and ubRMSD (global, no land cover classes)
    metrics = []
    for p in products:
        pair = f"0-ISMN_and_{p}"
        metrics.append((f'R_between_{pair}', '-', f'p_R_between_{pair}', 0.01))
    for p in products:
        pair = f"0-ERA5_LAND_and_{p}"
        metrics.append((f'R_between_{pair}', '-', f'p_R_between_{pair}', 0.01))
        metrics.append((f'urmsd_between_{pair}', 'm³m⁻³', None, 0.01))

    report_data, tracking_status = {}, []
    for metric, unit, p_mask, alpha in metrics:
        logger.info("[%s] Tracking metric: %s", report_name, metric)
        kwargs = dict(metric=metric, unit=unit, ref_epoch=report_name,
                      n_epochs=n_epochs, path_out=report_root / "tracking",
                      corridor_alpha=alpha, min_history=3)
        if metric.startswith('R_'):
            kwargs['pretty_name'] = 'R'
            kwargs['p_mask_var'] = p_mask
        series.track_metric(**kwargs)
        data = Data().from_yml(report_root / "tracking" / f"tracking_{metric}.yml")
        report_data[f"Tracking {metric}"] = data.data
        tracking_status.append(data.data['tracking']['tracking_status'])

    # ubRMSD vs. ISMN per land cover class (ESA CCI LC 2010 of the site),
    # with a fixed expected range (C3S accuracy requirement 0-0.1 m³m⁻³).
    # Must match \kpithres in main.tex.
    lc_classes = {
        'cropland': [10, 11, 12, 20, 30, 40],
        'treecover': [50, 60, 61, 62, 70, 71, 72, 80, 81, 82, 90, 100, 160, 170],
        'grassland': [110, 120, 121, 122, 130, 140, 150, 151, 152, 153, 180],
    }
    for product in products:
        lc_metric = f'urmsd_between_0-ISMN_and_{product}'
        for lc, codes in lc_classes.items():
            logger.info("[%s] Tracking metric: %s (%s)", report_name, lc_metric, lc)
            data = series.track_metric(
                metric=lc_metric, unit='m³m⁻³', ref_epoch=report_name,
                n_epochs=n_epochs, path_out=report_root / "tracking" / f"lc_{lc}",
                corridor_bounds=(0, 0.1),
                # keep only sensors in this land cover class
                preprocess=lambda ds, c=codes, m=lc_metric: ds.assign(
                    {m: ds[m].where(ds['lc_2010'].isin(c))}))
            report_data[f"Tracking {lc_metric}_lc_{lc}"] = data.data
            tracking_status.append(data.data['tracking']['tracking_status'])

    # Number of common observations per ERA5-Land grid point (the same for
    # all products, one run). ERA5-Land is gap-free and C3S COMBINED is the
    # temporal reference, so this detects gaps / outages in the satellite
    # data stream. Informative only, not part of the traffic light.
    logger.info("[%s] Tracking number of observations (ERA5-Land run)", report_name)
    nobs = series.track_metric(
        metric='n_obs', unit='-', pretty_name='Number of observations',
        ref_epoch=report_name, n_epochs=n_epochs,
        run='Run2',  # ERA5-Land run (run folder names end with RunX)
        title='ERA5-Land vs. all products',
        path_out=report_root / "tracking",
        corridor_alpha=0.01, min_history=3,
        # only grid points with valid results (status GOOD), otherwise the
        # many points without data (n_obs=0) dominate the distribution.
        # Locations that drop out completely are shown by the coverage.
        preprocess=lambda ds: ds.assign(n_obs=ds['n_obs'].where(
            ds['status_between_0-ERA5_LAND_and_1-C3S_combined'] == 0)))
    report_data['Nobs'] = nobs.data

    # Spatial coverage: number of ERA5-Land grid points with GOOD validation
    # status per product. ERA5-Land is gap-free, so changes reflect the
    # satellite data (unlike ISMN, where the in situ data availability
    # dominates). Informative only, not part of the traffic light.
    logger.info("[%s] Tracking spatial coverage (ERA5-Land run)", report_name)
    coverage = series.track_spatial_coverage(
        status_vars=[f'status_between_0-ERA5_LAND_and_{p}' for p in products],
        ref_epoch=report_name, n_epochs=n_epochs,
        path_out=report_root / "tracking")
    report_data['Coverage'] = coverage.data

    traffic_light = _traffic_light(tracking_status)
    logger.info("[%s] Traffic light: %s", report_name, traffic_light)

    compilation_date = datetime.now().strftime("%Y-%m-%d %H:%M")
    _d = compilation_date.replace("-", '').replace(' ', '').replace(":", '')
    filename = f"{interval_to.replace('-', '')}_{_d}_{traffic_light[0].upper()}.pdf"

    series[report_name].collect_content(
        report_variables={'traffic_light': traffic_light,
                          'series_name': series.name,
                          'report_name': report_name,
                          'compilation_date': compilation_date,
                          'filename': filename,
                          'n_epochs': n_epochs},
        report_data=report_data)

    logger.info("[%s] Compiling PDF report", report_name)
    series[report_name].compile(
        template_path=latex_templ_path,
        run_tex=['run_ismn.tex', 'run_era5land.tex'],
        tex_ignore=None)

    dest = report_collection / filename
    logger.info("[%s] Copying PDF to %s", report_name, dest)
    shutil.copy(report_root / 'pdf_report' / 'main.pdf', dest)
    logger.info("[%s] collect_and_compile done", report_name)


# The DAG runs on the 1st of each quarter (Jan, Apr, Jul, Oct) and builds the
# report for the quarter that just ended. C3S ICDR extensions are published
# every 10 days with a delay of 1-2 weeks, so the data for the end of the
# quarter is usually available after 2-4 weeks. We wait up to 100 days.
with DAG(
        "C3S_SM-v202605-Autoreport",
        default_args={
            "depends_on_past": True,
            "email": ["wolfgang.preimesberger@geo.tuwien.ac.at"],
            "email_on_failure": EMAIL_ON_FAILURE,
            "email_on_retry": EMAIL_ON_FAILURE,
            "retries": 3,
            "retry_delay": timedelta(hours=3),
        },
        description="Create C3S SM v202605 quarterly validation report",
        # Data-interval semantics: data_interval_end is the trigger date
        # (1st of the month after the quarter), used to derive the period.
        schedule=CronDataIntervalTimetable("0 0 1 1,4,7,10 *", timezone="UTC"),
        start_date=datetime(2025, 1, 1),
        catchup=True,
        max_active_runs=1,
        tags=["c3s", "v202605", "autoreport"],
) as dag:

    verify_service_access = PythonOperator(
        task_id="verify_service_access",
        python_callable=_verify_service_access,
        doc="Establish a connection to the instance from the global config "
            "using the token from the config. Will fail if the login as user "
            "does not work."
    )

    is_staging_required_branch = BranchPythonOperator(
        task_id="is_staging_required_branch",
        python_callable=_is_staging_required_branch,
        doc="Check if the report for this quarter already exists or if it "
            "should be staged in the next step."
    )

    stage_new_report = PythonOperator(
        task_id="stage_new_report",
        python_callable=_stage_new_report,
        doc="Stage a new report: create the report directory and place the "
            "run config files. No processing happens yet."
    )

    is_processing_required_branch = BranchPythonOperator(
        task_id="is_processing_required_branch",
        python_callable=_is_processing_required_branch,
        trigger_rule="none_failed_min_one_success",
        doc="Check if the validation runs must be triggered, are running, or "
            "have finished."
    )

    wait_for_data = PythonSensor(
        task_id="wait_for_data",
        python_callable=_sense_data_available,
        poke_interval=60 * 60 * 24,        # check every 24 hours
        timeout=60 * 60 * 24 * 100,        # give up after 100 days
        mode="reschedule",                 # frees up worker slot while waiting
        doc="Wait for the data of the full quarter to be available in the "
            "service (ICDR delay); checked once per day."
    )

    start_validation_runs = PythonOperator(
        task_id="start_validation_runs",
        python_callable=_start_validation_runs,
        doc="Trigger all validation runs asynchronously."
    )

    wait_for_validation = PythonSensor(
        task_id="wait_for_validation",
        python_callable=_sense_runs_finished,
        poke_interval=60 * 60 * 1,         # check every hour
        timeout=60 * 60 * 24 * 5,          # give up after 5 days
        mode="reschedule",
        trigger_rule="none_failed_min_one_success",
        doc="Wait for the validation runs on the server to finish. The global "
            "run against ERA5-Land can take a few days."
    )

    is_compiling_required_branch = BranchPythonOperator(
        task_id="is_compiling_required_branch",
        python_callable=_is_compiling_required_branch,
        trigger_rule="none_failed_min_one_success",
        doc="Check if the report still needs to be compiled."
    )

    collect_and_compile = PythonOperator(
        task_id="collect_and_compile",
        python_callable=_collect_and_compile,
        doc="Download the results, track the metrics and compile the PDF."
    )

    finish = EmptyOperator(
        task_id="finish",
        trigger_rule="none_failed_min_one_success",
        doc="No-op terminal task to close the DAG run cleanly.",
    )

    verify_service_access >> is_staging_required_branch
    is_staging_required_branch >> stage_new_report >> is_processing_required_branch
    is_staging_required_branch >> is_processing_required_branch
    is_processing_required_branch >> wait_for_data >> start_validation_runs >> wait_for_validation >> is_compiling_required_branch
    is_processing_required_branch >> wait_for_validation >> is_compiling_required_branch
    is_processing_required_branch >> is_compiling_required_branch
    is_compiling_required_branch >> collect_and_compile >> finish
    is_compiling_required_branch >> finish
