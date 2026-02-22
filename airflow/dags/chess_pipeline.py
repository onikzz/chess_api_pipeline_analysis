from airflow.decorators import dag, task
from datetime import datetime, timedelta

from extraction.extract_api import run_total
from include.transform_chess_data import run_silver
from include.transform_to_gold import gold_data

@dag(
    dag_id='chess_pipeline',
    start_date=datetime(2026, 2, 21),
    schedule='@daily',
    catchup=False,
    tags=['chess_project']
)
def chess_pipeline():
    
    @task
    def extraer_datos():
        run_total()
    
    @task
    def transformar_datos():
        run_silver()
    

    @task
    def procesar_datos():
        gold_data()
    
    extraer_datos() >> transformar_datos() >> procesar_datos()

dag = chess_pipeline()