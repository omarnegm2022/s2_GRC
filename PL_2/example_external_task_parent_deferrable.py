import os
from airflow.models.dag import DAG
from airflow.providers.standard.operators.bash import BashOperator #NOTE: airflow.operators DEPRECATED
from airflow.providers.standard.operators.python import PythonOperator #NOTE: airflow.operators DEPRECATED
from datetime import datetime

# from airflow.providers.postgres.hooks.postgres import psycopg2
#NOTE: Previously, PostgresOperator was used to perform this kind of operation. After deprecation this has been removed. Please use SQLExecuteQueryOperator instead.
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
# from s2_data_piplines.DBeaver import sql_dump

DAG_FOLDER = os.path.dirname(os.path.abspath(__file__))

#

import os
import json as js
# from s2_data_piplines.FF_prepare import ff_dict


# f"""{'n_l, '.join(map(str, [tuple(i) for i in tuple(f_vals)]))};"""

def sql_dump():
	ff_dict = dict()
	with open('/home/omar-negam/airflow/dags/s.json','r') as file:
		ff_dict = js.load(file)
	for src,v in ff_dict.items():
		q_mode = 'x'
		# with open(f"{src}/{src.split('/')[2]}.sql",'x') as scope_dml: #create & write
			# cols(main_schema.copy(), facts_schema.copy())
		for file_path,schema in v.items():
			query_p1 = f"""INSERT INTO "{src.split('/')[2]}" ({', '.join(map(str, schema))})"""
			print('\n',("/home/omar-negam/airflow/dags/s2_data_piplines/"+file_path).split('.')[1].strip(),'\n')
				# print(query_p1)
			records = []
			with open(("/home/omar-negam/airflow/dags/s2_data_piplines/"+file_path),'r') as rec:
				for line in rec:
					records.append(line[:-1].strip().split(','))

			query_p2 = f"""n_l VALUES {'n_l, '.join(map(str, [tuple(i) for i in tuple(records)] ))};""".replace('n_l','\n').replace(", ''",", NULL")#.replace(", '')",", NULL)")
					#NOTE: as the ESC chars are not valid in f-string.

			# print(query_p1 + query_p2)
			with open(f"/home/omar-negam/airflow/dags/s2_data_piplines/{src}/{src.split('/')[2]}.sql", q_mode) as scope_dml: #create & write
				scope_dml.write(f'\n-- s2_data_piplines/{file_path}\n')
				scope_dml.writelines(query_p1 + query_p2)
				# print(os.listdir(f"{src}/")[-1]," created.")
				q_mode = 'a'
		break
		# break


		# scope_dml.writelines()

#
 
with DAG(
    dag_id='my_new_dag',
    start_date=datetime(2025, 7, 28), # Make sure this is in the past!
    # schedule=None, # Or "@daily", "0 0 * * *"
    catchup=False,
    tags=['my_project'],
) as dag:
	
    # task11 = BashOperator(
    #     task_id='hello',
    #     bash_command='Scripts/try.sh',
    # )

    task1 = PythonOperator(
        task_id='sql_dump',
        python_callable=sql_dump,
    )

    task2 = SQLExecuteQueryOperator(
    task_id='run_query2',

    sql="Scope1_2.sql",#NOTE: It M_U_S_T be in the S_A_M_E `dags_folder`. See `airflow config list` in the BashShell. 
    
    conn_id='my_postgres_conn',  # ← This refers to the stored connection, from the Airflow web UI.
    dag=dag
)
    task4 = SQLExecuteQueryOperator(
    task_id='run_query4',
    sql="Scope1_4.sql",
    conn_id='my_postgres_conn'
)
    task6 = SQLExecuteQueryOperator(
    task_id='run_query6',

    sql="Scope1_6.sql", 
    
    conn_id='my_postgres_conn'
)
    
    task7 = SQLExecuteQueryOperator(
    task_id='check_queries',

    sql="""
	        (SELECT * FROM s2_schema."scope_1" 
            WHERE "Level 1" = 'Bioenergy'
            LIMIT 1)
        UNION 
            (SELECT * FROM s2_schema."scope_1" 
            WHERE "Level 1" = 'Passenger vehicles'
            LIMIT 1)
        UNION
            (SELECT * FROM s2_schema."scope_1" 
            WHERE "Level 1" = 'SECR kWh pass & delivery vehs'
            LIMIT 1)
            """,
    
    conn_id='my_postgres_conn',
    dag=dag
)
    task1 >> [task2, task4, task6]
    [task2, task4, task6] >> task7




# The 'dag' variable must be accessible globally after the file is executed.
def process_results(**context):
    # Pull the result from XCom
    results = context['ti'].xcom_pull(task_ids='run_query')
    # for row in results:
    #     print(f'\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n{row}\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n')  # e.g., (1, 'Alice'), (2, 'Bob')
    print(os.listdir(f"{DAG_FOLDER}/s2_data_piplines/PL_1/Scope 1/")),