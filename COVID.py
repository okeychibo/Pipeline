import os
import wget
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from time import time
from sqlalchemy import create_engine
from airflow.decorators import task

# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2023, 9, 9),
#     'retries': 1
# }
dag = DAG(dag_id='covid',
          start_date=datetime(2022, 12, 9))

@task
def download_data():
    # print("DOWNLOAD DATA IS WORKING")

    final_df = []
    # Initialize an empty list to store months
    months = ['01', '02', '03', '04','05','06','07','08','09','10','11','12']
    # Loop to generate 12 months
    # for i in range(1, 13):
    #     # Format the month to have leading zeros if needed
    #     formatted_month = f"{i:02d}"
    #     # Append the formatted month to the list
    #     months.append(formatted_month)
    #     print(months)

    for data in months:
        result = wget.download(f"https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/{data}-01-2022.csv")
        df = pd.read_csv(result)
        final_df.append(df)
        df_final = pd.concat(final_df)
        df_final.to_csv('first_twelve_months.csv', sep='|', index=False)
    print(df_final.head())

@task       
def local_to_postgres():
    
    engine = create_engine('postgresql://root:root@postgres_miracle:5432/ny_taxi')

    while True:

        df_iter = pd.read_csv('first_three_months.csv', sep='|', chunksize=100000)

        t_start = time()

        df = next(df_iter)
             
        df.to_sql(name='COVID_19', con=engine, if_exists='append')

        t_end = time()

        print('inserted another chunk, took %.3f second' % (t_end - t_start))

    print(df)

with dag:
    DOWNLOAD_DATA = download_data()

    LOCAL_TO_POSTGRES = local_to_postgres()

DOWNLOAD_DATA >> LOCAL_TO_POSTGRES
