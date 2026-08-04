# Employee ETL Demo — Apache Airflow Setup Guide

This walks you through everything Abhay asked for in the meeting, using
**Apache Airflow 3.3** (the current stable release, as of writing) as the
local scheduler:

1. A heavier dataset (5,000 employee records) run through a real ETL
2. Auto-scheduling — no manual triggering
3. **Three dependent ETL workflows**, where #2's input is #1's output and
   #3's input is #2's output
4. No hardcoded paths or business values anywhere in the code

Everything is explained from first principles, so read it top to bottom
the first time even if a section looks like something you already know.

---

## Part 1 - What Airflow actually is (read this before installing anything)

Airflow is a program that runs other programs, on a schedule, and keeps a
record of what happened. That's it. Four ideas explain almost everything:

| Concept | What it means here |
|---|---|
| **DAG** (Directed Acyclic Graph) | One workflow. In this project, each of the 3 ETL stages is its own DAG — its own Python file describing "what to run and in what order." |
| **Task** | One step inside a DAG. Each of our DAGs has exactly one task (run the Python script for that stage), but a DAG can have many. |
| **Operator** | A reusable template for a task. We use `BashOperator`, which just runs a shell command. There are hundreds of others (run SQL, call an API, wait for a file, etc.) but you only need this one for now. |
| **Scheduler** | The Airflow process that decides *when* each DAG's next run should happen, and hands tasks off to be executed. This is the "auto-scheduler" Abhay asked for — you configure it once, and it runs forever without you touching a button. |

Two more pieces you'll see when you run it, but don't need to write code for:

- **DAG processor** — continuously re-reads your DAG files from disk so
  Airflow notices changes.
- **API server** (called the "webserver" in older Airflow versions) — the
  UI you'll look at in your browser, plus the REST API.
- **Metadata database** — where Airflow stores DAG run history, task
  states, etc. For local use it's just a SQLite file; real deployments use
  Postgres.

For local development, one command (`airflow standalone`) starts all of
these at once. That's the "local scheduler, not cloud composer" Abhay
described.

### The key idea that makes the dependency chain work: Assets

Older Airflow tutorials show DAGs triggering each other with something
called `TriggerDagRunOperator` — DAG 1's last task explicitly says "now go
run DAG 2." It works, but it's brittle: DAG 1 has to know DAG 2 exists,
and if DAG 2 also needs DAG 3, that DAG has to know about DAG 3, and so on.

Airflow's modern answer (introduced as "Datasets," renamed **"Assets"** as
of Airflow 3.0) flips this around. Instead of a task saying "go trigger
that other DAG," a task says "I just updated *this piece of data*" (an
`Asset`), and any DAG that declared itself as needing that Asset gets
queued up automatically. Nobody has to know about anybody else — DAG 1
doesn't know DAG 2 exists, and DAG 2 doesn't know DAG 3 exists. Each just
declares what data it produces or needs.

This maps **exactly** onto what Abhay described in the meeting: *"the
input of the second ETL would be the output of the first ETL."* That's the
literal definition of asset-based scheduling. So instead of manually
wiring "trigger the next DAG," each stage just says:

- Stage 1 task: "when I finish, `stage1_output.csv` has been updated" (an *outlet*)
- Stage 2 DAG: "run me whenever `stage1_output.csv` is updated" (its *schedule*)
- Stage 2 task: "when I finish, `stage2_output.csv` has been updated" (an *outlet*)
- Stage 3 DAG: "run me whenever `stage2_output.csv` is updated" (its *schedule*)

That's the entire "dependency tree" Abhay asked you to build.

---

## Part 2 — What's in this project

```
employee_etl_airflow/
├── setup_env.sh                     ← source this every session (sets paths, no hardcoding)
├── scripts/
│   └── generate_employee_data.py    ← your existing generator, unchanged
├── data/
│   ├── raw/employee_data.csv        ← sample 5,000-row input (already generated for you)
│   ├── staging/                     ← stage 1 & 2 write here
│   └── final/                       ← stage 3's final deliverables land here
├── etl/                             ← the actual transformation logic (plain Python + pandas)
│   ├── etl_common.py                ← shared extract()/load() helpers
│   ├── stage1_extract_clean.py      ← ETL #1
│   ├── stage2_calculate_pay.py      ← ETL #2
│   └── stage3_finalize_load.py      ← ETL #3
└── dags/                            ← Airflow orchestration (imports nothing from etl/*.py directly —
    ├── dag_stage1_extract_clean.py     it calls each script as a subprocess, exactly like you'd run
    ├── dag_stage2_calculate_pay.py     it by hand from the terminal)
    └── dag_stage3_finalize_load.py
```

**A deliberate design call, stated explicitly (an actual SWE always writes
this down rather than leaving it implicit):** your existing `etl_job.py`
uses PySpark. I kept the *pattern* (generic `extract()`/`transform()`/
`load()`, argparse, no hardcoded values) but swapped the engine to plain
pandas for this project. Spark's whole reason to exist is splitting work
across a cluster of machines — for 5,000 rows on one laptop, it adds a
Java dependency and startup overhead for zero benefit, and would compete
for your attention with the actual thing you're here to learn (Airflow).
**If you specifically need to keep Spark** (e.g., your manager expects to
see `spark-submit` in the demo), see **Part 6** — the DAGs don't change at
all, only one line inside each `BashOperator`'s command.

### The business logic in each stage

- **Stage 1 (`stage1_extract_clean.py`)** — drops rows missing required
  fields or with non-positive salaries, de-duplicates, enforces correct
  data types, stamps a `stage1_processed_at` timestamp.
- **Stage 2 (`stage2_calculate_pay.py`)** — calculates `monthly_salary`
  (salary ÷ 12) and `vacation_days` using an age-banded accrual rule. ⚠️
  Your dataset has no hire-date/tenure column, so a real "years of
  service" rule isn't possible — this uses age as a stand-in and says so
  in the code comments. All four thresholds (base days, bonus interval,
  starting age, cap) are configurable — see Part 5.
- **Stage 3 (`stage3_finalize_load.py`)** — rounds money fields, buckets
  employees into age brackets, and writes **two** files to `data/final/`
  (a different location than staging, as requested): the full finalized
  dataset, and a small `salary_summary.csv` aggregate report — the kind of
  rollup a billing system would actually want.

I already ran all three stages against your real 5,000-row file to verify
the logic works end-to-end before handing this to you — `data/staging/`
and `data/final/` already contain the real output, so you have something
to compare against once you run it yourself through Airflow.

---

## Part 3 — Installing Airflow locally

Airflow 3.3.0 requires Python 3.10–3.14. Check yours:

```bash
python3 --version
```

Create an isolated virtual environment (never install Airflow into your
system Python):

```bash
cd employee_etl_airflow
python3 -m venv airflow_venv
source airflow_venv/bin/activate      # run this every new terminal session too
```

Install Airflow using the official constraints file (this pins every
dependency to versions known to work together — skipping it is the #1
cause of confusing install errors):

```bash
AIRFLOW_VERSION=3.3.0
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" pandas --constraint "${CONSTRAINT_URL}"
```

(`pandas` is installed into the same venv because the `BashOperator` tasks
run `python3 ...` using whatever Python is first on the PATH — which, once
you've activated this venv, is this venv's Python.)

---

## Part 4 — Running it

Every new terminal session, in the project folder:

```bash
source airflow_venv/bin/activate
source setup_env.sh
```

`setup_env.sh` prints out the paths it set — confirm they look right.
This is the **only** place any path is ever typed as a literal string;
everything else reads it from the environment.

Start Airflow (first run auto-creates the database and an admin user, and
prints the generated password to your terminal — copy it):

```bash
airflow standalone
```

Leave this running. Open **http://localhost:8080** in your browser and
log in with username `admin` and the password from the terminal output.

You should see three DAGs: `employee_etl_stage1_extract_clean`,
`employee_etl_stage2_calculate_pay`, `employee_etl_stage3_finalize_load`.

**Unpause all three** using the toggle on the left of each row — a paused
DAG is loaded but will never actually run, whether by schedule, asset
trigger, or manual click. This trips up almost everyone the first time.

### Trigger the chain

Click into `employee_etl_stage1_extract_clean` and hit the ▶ "Trigger DAG"
button (or wait for its `@daily` schedule). Watch what happens:

1. Stage 1 runs, turns green, and updates its Asset.
2. Within a few seconds, **stage 2 automatically appears as a new,
   queued/running DAG run** — nobody clicked anything. Check the "Assets"
   tab in the left nav to see the event that caused it.
3. Once stage 2 finishes, **stage 3 automatically triggers** the same way.

Then check your filesystem:

```bash
cat data/final/employee_final.csv | head -5
cat data/final/salary_summary.csv
```

That's the full dependency tree working exactly as described in the
meeting — auto-triggered, no manual scheduling between stages.

---

## Part 5 — "Plug and play": changing behavior without touching code

Go to **Admin → Variables** in the UI and add:

| Key | Example value |
|---|---|
| `BASE_VACATION_DAYS` | `12` |
| `YEARS_PER_BONUS_DAY` | `4` |
| `BONUS_AGE_START` | `28` |
| `MAX_VACATION_DAYS` | `30` |

Re-trigger stage 1 (which cascades through 2 and 3). Stage 2's task reads
these through a Jinja template (`{{ var.value.get('BASE_VACATION_DAYS', 10) }}`)
at the moment it *runs* — not when the DAG file is parsed — so the change
takes effect on the very next run with **zero code changes and no
restart**. This is the concrete version of "avoid the hardcoded things,
make it generic, plug and play" from the meeting: business policy lives
in Airflow's UI, not buried in source code.

(If you don't set these Variables at all, each has a sensible default
baked into the DAG's Jinja template, so the pipeline still works
out-of-the-box.)

---

## Part 6 — If you need to keep PySpark instead of pandas

Nothing about the DAG structure changes — only the `bash_command` inside
each `BashOperator`. For example, stage 1 would become:

```python
run_stage1 = BashOperator(
    task_id="run_stage1_extract_clean",
    bash_command=(
        "spark-submit {{ params.code_dir }}/etl_job.py "
        "--source_type csv --source_path {{ params.data_dir }}/raw/employee_data.csv "
        "--target_type csv --target_path {{ params.data_dir }}/staging/stage1_output.csv"
    ),
    params={"data_dir": DATA_DIR, "code_dir": CODE_DIR},
    outlets=[STAGE1_OUTPUT_ASSET],
)
```

You'd need Java + Spark installed and on PATH wherever `airflow standalone`
runs, and you'd extend `etl_job.py`'s `transform()` function with the
salary/vacation logic (using an extra `--stage` argparse flag to pick
which transform to run, so it stays one generic script rather than three
copies). Everything else in this guide — Assets, Variables, the UI, the
auto-triggering — is identical.

---

## Part 7 — Common issues

- **Stage 2/3 never trigger** → almost always because one of the DAGs is
  still paused. Check all three toggles in the UI.
- **`FileNotFoundError` in the task log** → `EMPLOYEE_ETL_DATA_DIR` wasn't
  set in the terminal where you ran `airflow standalone` (you must
  `source setup_env.sh` in *that* terminal before starting it).
- **`ModuleNotFoundError: pandas`** → pandas isn't installed in whichever
  Python `airflow standalone` is actually using — confirm with
  `which python3` while your venv is active, and that it matches what
  Airflow's task logs show.
- **A run stays queued forever** → the scheduler component isn't running.
  `airflow standalone` starts it for you automatically; if you split into
  `airflow scheduler` / `airflow api-server` / `airflow dag-processor`
  manually, make sure all of them are actually up.

---

## Part 8 — Where this goes next (for the billing project)

A few upgrades worth knowing about once this demo is solid, since Abhay
flagged this as prep for the billing project:

- **Real scheduling**: change stage 1's `schedule="@daily"` to a cron
  string, or trigger it from an external event via the REST API instead
  of a fixed time.
- **Retries/alerting**: `default_args={"retries": 2, "retry_delay": ...}`
  and `on_failure_callback=` for Slack/email alerts when a stage fails.
- **Production deployment**: swap SQLite for Postgres, and run
  `airflow scheduler`, `airflow api-server`, and `airflow dag-processor`
  as separate long-running services (e.g., via Docker Compose) instead of
  the all-in-one `airflow standalone` command, which is explicitly
  documented as dev-only.
