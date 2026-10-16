"""Small manual DAG proving LocalExecutor + Execution API work."""
from datetime import datetime, timezone
from airflow.sdk import dag, task


@dag(
    dag_id="homelab_smoke_test",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    tags=["handyman", "smoke-test"],
)
def homelab_smoke_test():
    @task()
    def ping():
        print("Airflow 3 LocalExecutor: task executed successfully")
        return "OK"

    ping()


homelab_smoke_test()
