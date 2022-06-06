#
# Using a non-whitelisted operator
#

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.version import version
from datetime import datetime, timedelta


def provisioning_python(ts,**kwargs):
    """
    This can be any python code you want and is called from the python operator. The code is not executed until
    the task is run by the airflow scheduler.
    """
    print(f"Provisioning infras {kwargs['cluster_config']}.")
    print(kwargs)


def compliance_check(ts,**kwargs):
    """
    This can be any python code you want and is called from the python operator. The code is not executed until
    the task is run by the airflow scheduler.
    """
    print(f"Compliance check {kwargs['resources']}.")
    print(kwargs)


def deprovision_python(ts,**kwargs):
    """
    This can be any python code you want and is called from the python operator. The code is not executed until
    the task is run by the airflow scheduler.
    """
    print(f"DeProvisioning infras {kwargs['cluster_id']}.")
    print(kwargs)


# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

# Using a DAG context manager, you don't have to specify the dag property of each task
with DAG(
        dag_id="blacklist_operator-dag",
        start_date=pendulum.today('UTC').subtract(days=1),
        max_active_runs=3,
        schedule_interval=timedelta(minutes=30),
        default_args=default_args,
    ) as dag:

    t0 = EmptyOperator(
        task_id='empty_task_t0'
    )
    t1 = EmptyOperator(
        task_id='empty_task_t1'
    )
    t2 = BashOperator(
        task_id='bash_print_date1',
        bash_command='sleep $[ ( $RANDOM % 30 )  + 1 ]s && date')
    t3 = BashOperator(
        task_id='bash_print_date2',
        bash_command='sleep $[ ( $RANDOM % 30 )  + 1 ]s && date')

    ProvisionTask = PythonOperator(
        task_id=f'provision_cluster',
        python_callable=provisioning_python,  
        op_kwargs={'cluster_config': 'someconfig'},
    )
    
    ComplianceTask = PythonOperator(
        task_id=f'compliance_check',
        python_callable=compliance_check,  
        op_kwargs={'resources': 'list of resources'},
    )

    DeProvisionTask = PythonOperator(
        task_id=f'deprovision_cluster',
        python_callable=deprovision_python,
        op_kwargs={'cluster_id': 'cluster ID'},
    )

    # Attempts to use a non-whitelisted operator   
    MySqlTask = MySqlOperator(
        sql=""" CREATE TABLE dezyre.employee(empid int, empname VARCHAR(25), salary int); """, 
        task_id="CreateTable", 
        mysql_conn_id="mysql_conn",
    )

    # Simulate provisioning infras, running compliance tests, running ETL jobs and then deprovisioning the infras
    ProvisionTask >> ComplianceTask >> t0 >> t1 >> t2 >> t3 >> MySqlTask >> DeProvisionTask