from airflow.decorators import task, task_group

@task.python
def process_a(partner_name, partner_path):
    print(partner_name)
    print(partner_path)

@task.python
def process_b(partner_name, partner_path):
    print(partner_name)
    print(partner_path)

@task.python
def process_c(partner_name, partner_path):
    print(partner_name)
    print(partner_path)

@task.python
def check_a():
    print('check_a')

@task.python
def check_b():
    print('check_b')

@task.python
def check_c():
    print('check_c')

@task_group(group_id='checks')
def checks_group():
    check_a()
    check_b()
    check_c()


@task_group()
def process_tasks(partner_settings):
    # Tasks for processing
    a = process_a(partner_settings['partner_name'], partner_settings['partner_path'])
    b = process_b(partner_settings['partner_name'], partner_settings['partner_path'])
    c = process_c(partner_settings['partner_name'], partner_settings['partner_path'])
    
    # Sub-task group for checks
    checks = checks_group()
    
    # Define dependencies
    a >> checks
    b >> checks
    c >> checks

"""
def process_tasks(partner_settings):
    with TaskGroup(group_id='process_tasks') as process_tasks:
        with TaskGroup(group_id='checks') as checks:
            check_a()
            check_b()
            check_c()
        process_a(partner_settings['partner_name'], partner_settings['partner_path']) >> checks
        process_b(partner_settings['partner_name'], partner_settings['partner_path']) >> checks
        process_c(partner_settings['partner_name'], partner_settings['partner_path']) >> checks
        
    return process_tasks
"""

