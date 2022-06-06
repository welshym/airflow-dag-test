from airflow.models import DagBag
from modulefinder import ModuleFinder
import os 

dag_bag = DagBag(dag_folder='dags/', include_examples=False)
for dag in dag_bag.dags:
    print ("*************************")
    print ("DAG Print")
    print ("*************************")
    print (dir(dag_bag.dags[dag]))
    print ("*************************")
    print (dag_bag.dags[dag].tasks)
    print ("*************************")
    print (dag)
    print ("*************************")
    print (dag_bag.dags[dag].task_dict)
    print ("*************************")
    print (dag_bag.dags[dag].has_task('deprovision_cluster'))
    print ("*************************")


    provisionTask = dag_bag.dags[dag].get_task('provision_cluster')
    upstream_task_ids = list(map(lambda task: task.task_id, provisionTask.upstream_list))
    print (upstream_task_ids)

    downstream_task_ids = list(map(lambda task: task.task_id, provisionTask.downstream_list))
    print (downstream_task_ids)

    deProvisionTask = dag_bag.dags[dag].get_task('deprovision_cluster')
    upstream_task_ids = list(map(lambda task: task.task_id, deProvisionTask.upstream_list))
    downstream_task_ids = list(map(lambda task: task.task_id, deProvisionTask.downstream_list))
    print (upstream_task_ids)
    print (downstream_task_ids)
    
    finder = ModuleFinder()
    finder.run_script(os.getcwd() + '/dags/' + dag + '.py')

    print('Loaded modules:')
    usedOperatorsList = []
    whitelistOperatorsList = ['airflow.operators.python', 'airflow.operators.empty']
    operatorSubstr = 'airflow.operators.'
    for name, mod in finder.modules.items():
        #print('%s: ' % name, end='')
        #print(','.join(list(mod.globalnames.keys())[:3]))
        if operatorSubstr in name:
            usedOperatorsList.append(name)

    for name, mod in finder.badmodules.items():
        if operatorSubstr in name:
            usedOperatorsList.append(name)

    print (usedOperatorsList)
    blacklistOperators = set(usedOperatorsList) - set(whitelistOperatorsList)
    print (blacklistOperators)
    
    print('-'*50)
    print('Modules not imported:')
    print('\n'.join(finder.badmodules.keys()))

    


# Must include tasks: Provision, Compliance and DeProvision
# Provision must not have any upstream tasks
# Provision must have compliance as downstream task
# DeProvision must not have any downstream tasks

# Check for DAG impoirt failures
# Check for cyclic errors (from airflow.utils.dag_cycle_tester import test_cycle

# Check for whitelisted operators only