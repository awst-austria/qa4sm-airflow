Keep the AGENTS.md file updated with any decisions and general information that affect this repository.

Automated validation reports track the data production by operational satellite soil moisture data streams.
Operational products are regularly updated on QA4SM.
Airflow triggers predefined validation runs (configuration files), downloads validation results and fill the latex templates with dynamic contents.

# Literature supporting the C3S automated validation report generation
- https://confluence.ecmwf.int/spaces/CCDW/pages/662515635/Soil+Moisture+v202605+Product+Quality+Assessment+Report+PQAR
- https://confluence.ecmwf.int/spaces/CCDW/pages/620427975/Soil+Moisture+v202605+Algorithm+Theoretical+Basis+Document+ATBD

# Setup decisions
- Docker access for DockerOperator DAGs goes ONLY through the `docker-proxy`
  (socat) service: DAGs use `docker_url='tcp://docker-proxy:2375'`. The Airflow
  containers run as `${AIRFLOW_UID}:0` and are not in the group of
  `/var/run/docker.sock` (root:docker, mode 660), so a direct socket mount does
  not work. Don't mount the socket into the Airflow containers and never publish
  port 2375 on the host (unauthenticated Docker API = root on the host).
  The alternative (`group_add` with the host's docker GID) was considered and
  not adopted.
- All packages in `base_requirements.txt` are pinned. Airflow-related versions
  follow the official Airflow constraints file (`AIRFLOW_CONSTRAINTS` in the
  `Dockerfile`), which is also passed to pip. To upgrade (e.g. a new
  qa4sm-autoreports release), bump the version there; `docker-run.sh` runs
  `docker compose up --build`, so only the pip layer is rebuilt.
  `qa4sm-api` is not pinned separately, it is installed via qa4sm-autoreports.
- Line endings: the repo uses `core.autocrlf=true`. Shell scripts must keep LF
  line endings (a checkout can turn them into CRLF, which breaks bash);
  the `Dockerfile` uses CRLF. Preserve the existing line endings when editing.

# Autoreport DAGs
- Each report series has its own DAG (`dags/autoreport/<series>_report.py`)
  and folder with `report_config_templates/` (one QA4SM validation config per
  run) and `report_latex_templates/src/`. Steps: stage -> trigger runs ->
  sensor (reschedule mode) -> collect, track, compile -> copy PDF.
- `$QA4SM_REPORTS_PATH/results/<series>` and `$QA4SM_REPORTS_PATH/pdf/<series>`
  must exist on the host, otherwise the DAG fails to import (assert).
- Run folders are named `<RUN_PREFIX> <report_name> Run<i>`; the templates and
  `track_metric(run='Run<i>')` (matched by suffix) rely on this.
- Templates contain no hard-coded results: all report-dependent values are
  placeholders `\detokenize{$<python expr>$}`, evaluated against `ReportVars`,
  `ContentVars` and `Run<i>ContentVars`. Values passed via `report_data` in the
  DAG (e.g. `Tracking <metric>`, `Coverage`, `Nobs`) are accessed as
  `ReportVars['<key>']`. Fixed values are only those set by definition
  (product version, KPI thresholds such as the 0.1 m³m⁻³ ubRMSD requirement,
  which must match `corridor_bounds` in the DAG).
- Traffic light: built from the tracking status of the tracked metrics
  (statistical corridor, `corridor_alpha=0.01`, `min_history=3`; fixed bounds
  where a requirement exists). n_obs and spatial coverage are informative only.
- Spatial coverage is counted from the `status_between_...` variables
  (status 0 = GOOD), not from metric variables. It is taken from the run
  against a gap-free gridded reference (ERA5-Land), so that it reflects the
  satellite data and not the in situ data availability (SMOS and C3S).
- n_obs is the same for all pairs of a run (temporal matching to the temporal
  reference), so it is tracked from one run only: ERA5-Land for SMOS (gap-free,
  so gaps in n_obs show SMOS outages), also ERA5-Land for C3S (temporal
  reference C3S COMBINED). Like the coverage, n_obs is taken from the gap-free
  gridded run, not from ISMN (in situ gaps would mask satellite outages).
  n_obs is masked to locations with status GOOD (`preprocess`), otherwise the
  many locations without data (n_obs=0) make the median 0.
- Figure macros `\mapfig` / `\plotfig` (main.tex) fit maps into a fixed box, so
  regional (portrait) and global (landscape) maps both work next to the
  bar/box plots. In LaTeX, don't combine `angle=-90` with `height=` in
  `\includegraphics` (pdflatex "Division by 0"), use `width=`.
- SMOS L2 v700: monthly, report covers the last 3 months, global configs.
  C3S v202605: quarterly, last complete quarter, global bbox set in the DAG
  (the configs have no bbox), `catchup=True` from 2025-01 builds the history.
- C3S report focuses on ISMN (Run1, `run_ismn.tex`): R tracked globally, ubRMSD
  per land cover class (KPI bounds). ERA5-Land (Run2, `run_era5land.tex`) is a
  short complementary chapter: R and ubRMSD tracked globally (statistical
  corridor, no KPI as ERA5-Land is not a reference truth), plus coverage.
  QA4SM returns no summary statistics (`SummaryVars`) for the large ERA5-Land
  result, so the template takes medians/IQRs from the tracking `series` of the
  current report. Keep the report concise; per-report details beyond this are
  in the online results.
- Every tracked metric (`Tracking ...` in `report_data`) counts in the summary
  and traffic light, so only track metrics that are shown in the report.
- pdflatex output is checked for `"! "` by qa4sm-autoreports: an overfull box
  that contains an `\ac{}` on the first pass (printed as `ECMWF!`) is treated
  as an error. Avoid long unbreakable URLs next to acronyms (use footnotes).
- Development: never stage or trigger runs on qa4sm.eu (production) for
  testing. Test the DAG callables against copies of existing report folders
  (with staging/triggering disabled) or against a local QA4SM instance.
