from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG("gridpulse_daily_billing", start_date=datetime(2026, 1, 1),
         schedule="* * * * *", catchup=False, max_active_runs=1,
         is_paused_upon_creation=False,
         default_args={"retries": 2, "retry_delay": timedelta(seconds=20)},
         description="Discover completed simulated days and rebuild authoritative bills") as dag:
    reconcile = BashOperator(task_id="reconcile_completed_days",
        bash_command="cd /app && python -m grid.remote_batch",
        execution_timeout=timedelta(minutes=5))
