# Must include tasks: Provision, Compliance and DeProvision (Done)
# Provision must not have any upstream tasks (Done)
# Provision must have compliance as downstream task (Done)
# DeProvision must not have any downstream tasks (Done)

# Check for DAG import failures (Done)
# Check for cyclic errors (Done)

# Check for whitelisted operators only

from airflow.models import DagBag
import logging
from modulefinder import ModuleFinder
import os 

LOGGER = logging.getLogger(__name__)

def test_no_import_errors():
  dag_bag = DagBag(dag_folder='dags/', include_examples=False)
  assert len(dag_bag.import_errors) == 0, "No Import Failures"

def test_retries_present():
  dag_bag = DagBag(dag_folder='dags/', include_examples=False)
  for dag in dag_bag.dags:
      retries = dag_bag.dags[dag].default_args.get('retries', [])
      error_msg = 'Retries not set to 2 for DAG {id}'.format(id=dag)
      assert retries == 2, error_msg

def test_compliance_config_valid():
  dag_bag = DagBag(dag_folder='dags/', include_examples=False)
  for dag in dag_bag.dags:
      provisionTask = dag_bag.dags[dag].get_task('provision_cluster')
      #LOGGER.info("Provisioning output")
      #LOGGER.info(provisionTask) 
      error_msg = 'Provisioning task not present for DAG {id}'.format(id=dag)
      assert provisionTask != None, error_msg

      upstream_task_ids = list(map(lambda task: task.task_id, provisionTask.upstream_list))
      error_msg = 'Provisioning task has parent tasks for DAG {id}'.format(id=dag)
      assert upstream_task_ids == [], error_msg

      downstream_task_ids = list(map(lambda task: task.task_id, provisionTask.downstream_list))
      error_msg = 'Provisioning task missing compliance child task for DAG {id}'.format(id=dag)
      assert downstream_task_ids == ['compliance_check'], error_msg

def test_deprovisioning_config_valid():
  dag_bag = DagBag(dag_folder='dags/', include_examples=False)
  for dag in dag_bag.dags:
      deProvisionTask = dag_bag.dags[dag].get_task('deprovision_cluster')

      error_msg = 'DeProvisioning task not present for DAG {id}'.format(id=dag)
      assert deProvisionTask != None, error_msg

      upstream_task_ids = list(map(lambda task: task.task_id, deProvisionTask.upstream_list))
      error_msg = 'DeProvisioning task has no parent tasks (therefore isnt in the DAG) for DAG {id}'.format(id=dag)
      assert upstream_task_ids != [], error_msg

      downstream_task_ids = list(map(lambda task: task.task_id, deProvisionTask.downstream_list))
      error_msg = 'DeProvisioning task has child tasks for DAG {id}'.format(id=dag)
      assert downstream_task_ids == [], error_msg


whitelistOperatorsList = ['airflow.operators.python', 'airflow.operators.empty', 'airflow.operators.bash', 'airflow.operators.trigger_dagrun', 'airflow.operators.subdag']

def test_whitelisted_operators():
  dag_bag = DagBag(dag_folder='dags/', include_examples=False)
  finder = ModuleFinder()
  for dag in dag_bag.dags:
    finder.run_script(os.getcwd() + '/dags/' + dag + '.py')
    usedOperatorsList = []
    operatorSubstr = 'airflow.operators.'
    for name, mod in finder.modules.items():
      if operatorSubstr in name:
        usedOperatorsList.append(name)

    for name, mod in finder.badmodules.items():
        if operatorSubstr in name:
            usedOperatorsList.append(name)

    blacklistOperators = set(usedOperatorsList) - set(whitelistOperatorsList)
    #print ("DAG: " + dag)
    #print ("blacklistOperators: ", blacklistOperators)
    #print ("usedOperatorsList: ", usedOperatorsList)
    error_msg = 'Blacklisted operators in use for DAG {id}'.format(id=dag)
    assert blacklistOperators == set(), error_msg

# Execute using: pytest -rpP test_dag_validation.py