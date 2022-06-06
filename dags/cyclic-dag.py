#
# Cyclic tasks failure DAG
#

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
        dag_id="cyclic_dag",
        start_date=pendulum.today('UTC').subtract(days=1),
        max_active_runs=3,
        schedule_interval=timedelta(minutes=30),
        default_args=default_args,
    ) as dag:

    start = EmptyOperator(
        task_id='start'
    )
    trigger_1 = EmptyOperator(
        task_id="dag_1",
    )
    trigger_2 = EmptyOperator(
        task_id="dag_2", 
    )
    some_other_task = EmptyOperator(
        task_id='some-other-task'
    )
    end = EmptyOperator(
        task_id='end'
    )
    
    # Creates a parser error by creating a cycle back to the start (purely job static check, not security)
    start >> trigger_1 >> some_other_task >> trigger_2 >> start 
