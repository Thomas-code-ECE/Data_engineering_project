import airflow
import datetime
import urllib.request as request
import pandas as pd
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
import requests
import random
import json
import glob
import time

DAGS_FOLDER = '/opt/airflow/dags/'
REQUEST_URL = 'https://www.dnd5eapi.co/api/'

default_args_dict = {
    'start_date': datetime.datetime.now(),
    'concurrency': 1,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
}

assignment_dag = DAG(
    dag_id='Data_Cleanse_Transform',
    default_args=default_args_dict,
    catchup=False,
    template_searchpath=DAGS_FOLDER,
)


def _work1(output_folder: str):

	#Loading kym.json file as a dataframe object
	df = pd.read_json(f'{output_folder}/kym.json')

	#Remove JSON object where the key category is different to Meme
	df = df.drop(df[df.category != "Meme"].index).reset_index(drop=True)

	#save to a new cleansed jason file
	df.to_json(f'{output_folder}/kym_cleaned1.json',orient = 'columns')

def _work2(output_folder: str):
	#Loading kym.json file as a dataframe object
	df = pd.read_json(f'{output_folder}/kym_cleaned1.json')

	#Remove the memes where the status is equal to DEADPOOL
	index_to_drop = []
	for index,element in enumerate(df.details):
		if(element['status'] == 'deadpool'):
			index_to_drop.append(index)
	df = df.drop(labels = index_to_drop,axis = 0).reset_index(drop=True)

	#save to a new cleansed jason file
	df.to_json(f'{output_folder}/kym_cleaned2.json',orient = 'columns')

def _work3(output_folder: str):
	#Loading kym.json file as a dataframe object
	df = pd.read_json(f'{output_folder}/kym_cleaned2.json')
	
	#Remove the column id 
	df.drop('ld', inplace=True, axis=1)

	#Remove the column detail
	df.drop('details', inplace=True, axis=1)

	#Remove the columns siblings
	df.drop('siblings', inplace=True, axis=1)

	#save to a new cleansed jason file
	df.to_json(f'{output_folder}/kym_cleaned.json',orient = 'columns')


def _work4(output_folder: str):
	#Loading kym.json file as a dataframe object
	df = pd.read_json(f'{output_folder}/kym_cleaned.json')

	#Remove the column meta and keep the column description
	description_value = []
	for index,element in enumerate(df.meta):
		if("description" in element):
			description_value.append(element["description"])
		else:
			description_value.append("unknown")
            
	df["description"] = description_value
	df.drop('meta', inplace=True, axis=1)

	#save to a new cleansed jason file
	df.to_json(f'{output_folder}/kym_transformed1.json',orient = 'columns')


def _work5(output_folder: str):
	#Loading kym.json file as a dataframe object
	df = pd.read_json(f'{output_folder}/kym_transformed1.json')

	# Converting the date of the last update and the posted date of the meme from UNIX timestamp to classic date 
	df['last_update_source'] = pd.to_datetime(df['last_update_source'],unit='s').dt.normalize()
	df['added'] = pd.to_datetime(df['added'],unit='s').dt.normalize()

	#Remove the meme posted before November 25, 2007 and after 2022
	df = df.drop(df[(df.added < '2007-11-25') & (df.added > '2022-01-01') & (df.added < '2007-11-25') & (df.added > '2022-01-01')].index)

	#save to a new cleansed jason file
	df.to_json(f'{output_folder}/kym_transformed2.json',orient = 'columns')


def _work6(output_folder: str):
	#Loading kym.json file as a dataframe object
	df = pd.read_json(f'{output_folder}/kym_transformed2.json')

	#Remove unicode characters from description
	description_value = []
	for index,element in enumerate(df.description):
		description_value.append(element.encode("ascii","ignore").decode())
	df["description"] = description_value

	#save to a new cleansed jason file
	df.to_json(f'{output_folder}/kym_transformed3.json',orient = 'columns')


def _work7(output_folder: str):
	#Loading kym.json file as a dataframe object
	df = pd.read_json(f'{output_folder}/kym_transformed3.json')

	#Set the tag words in lower case.
	tags_meme = []
	for index,element in enumerate(df.tags):
	    tags_meme.append(list(dict.fromkeys([tag.lower() for tag in element])))
	df["tags"] = tags_meme

	#Transform all desxription words to lower case.
	descriptions_keywords = []
	for index,element in enumerate(df.description):
		descriptions_keywords.append(element.lower())
	df["description"] = descriptions_keywords

	#save to a new cleansed jason file
	df.to_json(f'{output_folder}/kym_transformed4.json',orient = 'columns')

def _work8(output_folder: str):
	#Loading kym.json file as a dataframe object
	df = pd.read_json(f'{output_folder}/kym_transformed4.json')

	# Transform the links from the column template_image_url. Remove the first part "https://i.kym-cdn.com/entries/icons/original/000/" which is common to all memes
	template_url_meme = []
	for index,element in enumerate(df.template_image_url):
	    template_url_meme.append(element.split('original/000/')[1])
	df["template_image_url"] = template_url_meme 

	#Transform the links from the column url. Remove the first part "https://knowyourmeme.com/memes/" which is common to all memes
	url_meme = []
	for index,element in enumerate(df.url):
	    url_meme.append(element.split('/memes/')[1])
	df["url"] = url_meme

	#save to a new cleansed jason file
	df.to_json(f'{output_folder}/kym_transformed5.json',orient = 'columns')

def _work9(output_folder: str):
	#Loading kym.json file as a dataframe object
	df = pd.read_json(f'{output_folder}/kym_transformed5.json')

	#Transform the column children. Either the key gets children, create an array with the url where each link has been modified. In the same way as before, we delete the first part of the link which is redundant between the memes "https://knowyourmeme.com/memes/"
	df.loc[df['children'].isnull(),['children']] = df.loc[df['children'].isnull(),'children'].apply(lambda x: [])
	children_meme = []
	for index,element in enumerate(df.children):
		if(len(element) != 0):
			children_meme.append([children.split('/')[-1] for children in element])
		else:
			children_meme.append([])
	df["children"] = children_meme

	#Transform the column parents. Either the key gets parent, create an array with the url where each link has been modified. In the same way as before, we delete the first part of the link which is redundant between the memes "https://knowyourmeme.com/memes/"
	df.loc[df['parent'].notnull(),['parent']] = df.loc[df['parent'].notnull(),'parent'].apply(lambda x: x.split('/')[-1])
	df.loc[df['parent'].isnull(),['parent']] = df.loc[df['parent'].isnull(),'parent'].apply(lambda x: 'no parent')

	
	#save to a new cleansed jason file
	df.to_json(f'{output_folder}/kym_transformed6.json',orient = 'columns')

def _work10(output_folder: str):
	#Loading kym.json file as a dataframe object
	df = pd.read_json(f'{output_folder}/kym_transformed6.json')

	#Keep only the website of the references
	web_references_meme = []
	for index,element in enumerate(df.additional_references):
	    web_references_meme.append(list(element.keys()))
	df["additional_references"] = web_references_meme

	df.loc[df['search_keywords'].isnull(),['search_keywords']] = df.loc[df['search_keywords'].isnull(),'search_keywords'].apply(lambda x: [])
	search_keywords_meme= []
	for index,element in enumerate(df.search_keywords):
	    search_keywords_meme.append([''.join(filter(str.isalnum,keywords)) for keywords in element])
	df["search_keywords"] = search_keywords_meme
    
	df.drop('content', inplace=True, axis=1)

	df.drop('category', inplace=True, axis=1)

	df.to_json(f'{output_folder}/kym_transformed.json',orient = 'index')

node1 = PythonOperator(
    task_id='Remove_NON_Meme',
    dag=assignment_dag,
    trigger_rule='none_failed',
    python_callable=_work1,
    op_kwargs={"output_folder": DAGS_FOLDER},
    depends_on_past=False,
)

node2 = PythonOperator(
    task_id='Remove_DEADPOOL',
    dag=assignment_dag,
    trigger_rule='none_failed',
    python_callable=_work2,
    op_kwargs={"output_folder": DAGS_FOLDER},
    depends_on_past=False,
)

node3 = PythonOperator(
    task_id='Remove_useless_Columns',
    dag=assignment_dag,
    trigger_rule='none_failed',
    python_callable=_work3,
    op_kwargs={"output_folder": DAGS_FOLDER},
    depends_on_past=False,
)

node4 = PythonOperator(
    task_id='Adjust_Meta_Data',
    dag=assignment_dag,
    trigger_rule='none_failed',
    python_callable=_work4,
    op_kwargs={"output_folder": DAGS_FOLDER},
    depends_on_past=False,
)

node5 = PythonOperator(
    task_id='Remove_Old_MEMEs',
    dag=assignment_dag,
    trigger_rule='none_failed',
    python_callable=_work5,
    op_kwargs={"output_folder": DAGS_FOLDER},
    depends_on_past=False,
)

node6 = PythonOperator(
    task_id='Remove_unicode_characters',
    dag=assignment_dag,
    trigger_rule='none_failed',
    python_callable=_work6,
    op_kwargs={"output_folder": DAGS_FOLDER},
    depends_on_past=False,
)

node7 = PythonOperator(
    task_id='all_lowercase',
    dag=assignment_dag,
    trigger_rule='none_failed',
    python_callable=_work7,
    op_kwargs={"output_folder": DAGS_FOLDER},
    depends_on_past=False,
)

node8 = PythonOperator(
    task_id='Transform_the_links',
    dag=assignment_dag,
    trigger_rule='none_failed',
    python_callable=_work8,
    op_kwargs={"output_folder": DAGS_FOLDER},
    depends_on_past=False,
)

node9 = PythonOperator(
    task_id='Transform_children_parents',
    dag=assignment_dag,
    trigger_rule='none_failed',
    python_callable=_work9,
    op_kwargs={"output_folder": DAGS_FOLDER},
    depends_on_past=False,
)

node10 = PythonOperator(
    task_id='SAVE',
    dag=assignment_dag,
    trigger_rule='none_failed',
    python_callable=_work10,
    op_kwargs={"output_folder": DAGS_FOLDER},
    depends_on_past=False,
)

node1 >> node2 >> node3 >> node4 >> node5 >> node6 >> node7 >> node8 >> node9 >> node10
