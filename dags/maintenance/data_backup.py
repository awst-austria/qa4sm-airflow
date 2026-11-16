"""
Weekly incremental backup of all service data (QA4SM_DATA_PATH) to the
backup share (QA4SM_BACKUP_PATH) using rsync.

Only new or changed files (by size and modification time) are copied.
Files deleted from the source are NOT deleted from the backup.

Both paths are paths on the HOST machine. They are bind-mounted into a small
rsync container that is started through the docker-proxy, like the other
DockerOperator tasks. The data is mounted read-only.
"""
import logging
import os
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.providers.docker.operators.docker import DockerOperator, Mount

from misc import load_qa4sm_dotenv, log_command

load_qa4sm_dotenv()
QA4SM_DATA_PATH = os.environ["QA4SM_DATA_PATH"]  # On the HOST machine
# On the HOST machine, e.g. /home/<user>/shares/climers/Studies/QA4SM/05_data/SERVICE_DATA_BACKUP
QA4SM_BACKUP_PATH = os.environ.get("QA4SM_BACKUP_PATH", "")
EMAIL_ON_FAILURE = bool(int(os.environ.get("EMAIL_ON_FAILURE", 0)))

IMAGE = "alpine:3.20"

logger = logging.getLogger(__name__)

SRC = "/backup_src"
DST = "/backup_dst"

# - The share must already contain the target folder. If it is missing (e.g.
#   the share is not mounted), the bind mount fails and nothing is written to
#   the local disk.
# - -rt: recursive + keep modification times (needed to detect changed files).
#   Owner/group/permissions and symlinks are not copied, as the target is a
#   network share (CIFS) that does not support them.
# - --modify-window=2: CIFS stores timestamps with lower precision; avoids
#   re-copying unchanged files every week.
# - No --delete: files removed from the service stay in the backup.
# - rsync exit code 24 (source files vanished during the transfer, e.g. while
#   another DAG updates data) is treated as success.
_command = f"""sh -c '
set -u
[ -d {SRC} ] || {{ echo "Source {SRC} missing"; exit 1; }}
[ -d {DST} ] || {{ echo "Target {DST} missing"; exit 1; }}
touch {DST}/.write_test && rm {DST}/.write_test || {{ echo "Target {DST} not writable"; exit 1; }}
apk add --no-cache rsync >/dev/null || exit 1
rsync -rt --modify-window=2 --partial --stats --human-readable {SRC}/ {DST}/
rc=$?
[ $rc -eq 24 ] && echo "Some source files vanished during transfer (ignored)" && rc=0
exit $rc
'"""

with DAG(
        "Service-Data-Backup",
        default_args={
            "depends_on_past": False,
            "email": ["support@qa4sm.eu"],
            "email_on_failure": EMAIL_ON_FAILURE,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(hours=1),
        },
        description=f"Weekly incremental rsync backup of {QA4SM_DATA_PATH} "
                    f"to {QA4SM_BACKUP_PATH or '<QA4SM_BACKUP_PATH not set>'}",
        schedule="0 2 * * 0",  # Sundays 02:00 UTC
        start_date=datetime(2026, 11, 1),
        catchup=False,
        max_active_runs=1,  # never run two backups at the same time
        tags=["backup", "rsync", "maintenance"],
) as dag:
    if not QA4SM_BACKUP_PATH:
        # Fail loudly in the UI (import error) instead of silently doing nothing.
        raise ValueError("QA4SM_BACKUP_PATH is not set in qa4sm.env")

    DockerOperator(
        task_id="rsync_backup",
        image=IMAGE,
        container_name="task__maintenance__rsync_backup",
        command=_command,
        mounts=[
            Mount(target=SRC, source=QA4SM_DATA_PATH, type="bind", read_only=True),
            Mount(target=DST, source=QA4SM_BACKUP_PATH, type="bind"),
        ],
        auto_remove="force",
        mount_tmp_dir=False,
        docker_url="tcp://docker-proxy:2375",
        on_execute_callback=log_command,
        doc=__doc__,
    )
