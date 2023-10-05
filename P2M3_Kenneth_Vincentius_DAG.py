'''
=================================================
Milestone 3

Nama  : Kenneth Vincentius
Batch : HCK-007

Program ini dibuat untuk melakukan automatisasi transform dan load data dari PostgreSQL ke ElasticSearch dari data set tentang prediksi penyakit jantung
=================================================
'''
#import libary
from datetime import datetime
import psycopg2 as db
from elasticsearch import Elasticsearch
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd

#function untuk load data dari sql server airflow 
def load_data():
    conn_string = "dbname='airflow' host='postgres' user='airflow' password='airflow'"
    conn = db.connect(conn_string)
    df = pd.read_sql("select * from table_M3", conn)
    df.to_csv('P2M3_Kenneth_Vincentius_data_raw.csv',index=False)

#membuat fungsi untuk cleaning data
def cleaning_data():
    #mengload csv yang berhasil disimpan dari sql
    df = pd.read_csv('/opt/airflow/dags/P2M3_Kenneth_Vincentius_data_raw.csv')

    #mengubah tipe data di list kolom menjadi str atau object
    list_cat_columns = ['sex', 'cp', 'fbs', 'restecg', 'exng', 'slp', 'caa', 'thall', 'output']
    df[list_cat_columns] = df[list_cat_columns].astype(str)

    # Menggabungkan kolom numerical dan categorical dan mengurutkannya
    new_column_order = ['age', 'sex', 'cp', 'trtbps', 'chol', 'fbs', 'restecg', 'thalachh','exng', 'oldpeak', 'slp', 'caa', 'thall', 'output']
    df_fixed = df[new_column_order]

    # Rename kolom output dan ubah isi kolom categorical
    df_fixed.rename(columns={'output': 'heart_attack'}, inplace=True)
    df_fixed['sex'] = df_fixed['sex'].str.replace('0', 'male').str.replace('1', 'female')
    df_fixed['cp'] = df_fixed['cp'].str.replace('0', 'typical angina').str.replace('1', 'atypical angina').str.replace('2', 'non-anginal pain').str.replace('3', 'asymptomatic')
    df_fixed['fbs'] = df_fixed['fbs'].str.replace('0', 'False').str.replace('1', 'True')
    df_fixed['restecg'] = df_fixed['restecg'].str.replace('0', 'Normal').str.replace('1', 'ST-T Abnormal').str.replace('2', 'LVH')
    df_fixed['exng'] = df_fixed['exng'].str.replace('0', 'False').str.replace('1', 'True')
    df_fixed['slp'] = df_fixed['slp'].str.replace('0', 'Sloping').str.replace('1', 'Flat').str.replace('2', 'Upward Diagnosis')
    df_fixed['thall'] = df_fixed['thall'].str.replace('0', 'None (Normal)').str.replace('1', 'Fixed Defect').str.replace('2', 'Reversible Defect').str.replace('3', 'Thalassemia')
    df_fixed['heart_attack'] = df_fixed['heart_attack'].str.replace('0', 'False').str.replace('1', 'True')

    # Menghapus baris dengan indeks yang duplikat
    df_final = df_fixed.drop_duplicates(keep='first')

    #menyimpan data yang sudah di clean
    df_final.to_csv('/opt/airflow/dags/P2M3_Kenneth_Vincentius_data_clean.csv', index=False)

#membuat function untuk push ke elastic search
def push_es():
    #mendefine elastic search dan menload csv clean ke elastic search
    es = Elasticsearch("http://elasticsearch:9200")
    df_final_m3=pd.read_csv('/opt/airflow/dags/P2M3_Kenneth_Vincentius_data_clean.csv')

    #Indeksasi data ke Elasticsearch
    for i, r in df_final_m3.iterrows():
        doc = r.to_json()
        res = es.index(index="df_final_m3", body=doc)
        print(res)

#mendefine default args
default_args= {
    'owner': 'Kenneth',
    'start_date': datetime(2023, 9, 29) }

#mendefine dag agar bisa dibaca di ariflow
with DAG(
    "Milestone3",
    description='Milestone3',
    schedule_interval='@yearly',
    default_args=default_args, 
    catchup=False) as dag:

    #mendefine task 1 di airflow yaitu function load data dari sql
    load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_data

    )

    #mendefine task 2 di airflow yaitu clean data csv  
    cleaning_data = PythonOperator(
        task_id='cleaning_data',
        python_callable=cleaning_data

    )

    #mendefine task 3 di airflow yaitu menpoush ke dalam elasticsearch 
    push_es = PythonOperator(
        task_id='push_es',
        python_callable=push_es

    )

    #urutan rangkaian perkerjaan dari task airflow
    load_data >> cleaning_data >> push_es