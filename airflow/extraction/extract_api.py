import requests
import boto3 
import json
import os
import time 
from dotenv import  load_dotenv

load_dotenv()

api_user = {"User-Agent": "diegoreyes9821@gmail.com"}
s3 = boto3.client('s3')
bucket_raw = os.environ.get('s3_bucket_raw')
titulos = ['NM','WGM','WIM','WFM','WNM','CM','GM','WCM','IM','FM']

def getUsersListed():
    
    url = 'https://api.chess.com/pub/titled/'
    
    
    if not bucket_raw:
        print('check your .env')
        return
    
    for titulo in titulos:
    
        url_nueva = url + titulo
        response = requests.get(url_nueva, headers=api_user)
    
        if response.status_code == 200:
            data = response.json()
            json_string = json.dumps(data)

            print(f'Subiendo data de {titulo} a S3')

            s3.put_object(Bucket = bucket_raw, Key = f'raw/datos_{titulo}.json', Body = json_string)
        else:
            print(f'Error en el archivo de {titulo}, codigo: {response.status_code}')
            
        time.sleep(0.5)

def getUsersFromJsonS3():
        for titulo in titulos:
            key = f'raw/datos_{titulo}.json'
            batch_size = 100 
            lista_usuarios = []
            batch_count = 1
            
            try:
                response = s3.get_object(Bucket = bucket_raw, Key = key)
                content = response['Body'].read().decode('utf-8')
                data = json.loads(content)
                jugadores = data.get('players', [])
                
                url = 'https://api.chess.com/pub/player/'
                
                for jugador in jugadores:
                    response = requests.get(url= url + jugador, headers=api_user)
                    if response.status_code == 200:
                        lista_usuarios.append(response.json())
                    if len(lista_usuarios) == batch_size:
                        print(f"Lote {batch_count} lleno. Subiendo a S3")
                        s3.put_object(Bucket = bucket_raw, Key = f'raw/detalles/{titulo}/batch_{batch_count}.json', Body = json.dumps(lista_usuarios, indent=4))
                        lista_usuarios = []
                        batch_count += 1
                    time.sleep(0.05)
                
                if len(lista_usuarios) > 0:
                    print("Subiendo jugadores restantes")
                    s3.put_object(Bucket=bucket_raw, Key=f'raw/detalles/{titulo}/batch_{batch_count}.json', Body=json.dumps(lista_usuarios, indent=4))  
            except Exception as e:
                print(f'Error en titulo:{titulo}, {e}')             

def getStatsFromJsonS3():
    for titulo in titulos:
        prefix = f'raw/detalles/{titulo}/'
        stats_batch_size = 50 
        
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket_raw, Prefix=prefix):
            if 'Contents' not in page:
                continue
                
            for obj in page['Contents']:
                key = obj['Key']
                print(f"Procesando usuarios de {key} para extraer Stats...")
                
                response = s3.get_object(Bucket=bucket_raw, Key=key)
                jugadores_detalle = json.loads(response['Body'].read().decode('utf-8'))
                
                lista_stats = []
                for jugador in jugadores_detalle:
                    username = jugador.get('username')
                    if not username:
                        continue
                    
                    url_stats = f'https://api.chess.com/pub/player/{username}/stats'
                    res = requests.get(url_stats, headers=api_user)
                    
                    if res.status_code == 200:
                        stats_data = res.json()
                        stats_data['username'] = username
                        lista_stats.append(stats_data)
                    
                    time.sleep(0.05) 

                    if len(lista_stats) == stats_batch_size:
                        batch_id = int(time.time()) 
                        s3.put_object(
                            Bucket=bucket_raw, 
                            Key=f'raw/stats/{titulo}/stats_batch_{batch_id}.json', 
                            Body=json.dumps(lista_stats, indent=4)
                        )
                        print(f"Subido lote de stats para {titulo}")
                        lista_stats = []

                if lista_stats:
                    s3.put_object(
                        Bucket=bucket_raw, 
                        Key=f'raw/stats/{titulo}/stats_final_{int(time.time())}.json', 
                        Body=json.dumps(lista_stats, indent=4)
                    )

def run_total():
    print("Extraccion de datos")
    getUsersListed()
    getUsersFromJsonS3()
    getStatsFromJsonS3()
    print("Extraccion de datos finalizada")

if __name__=="__main__":
    run_total()