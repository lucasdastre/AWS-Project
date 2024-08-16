import pandas as pd
import boto3
import os
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv


# Carregar variáveis de ambiente
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')


def procces_csv(csv_file: str, output_dir: str, bucket_name: str, s3_key: str) -> None:
    os.makedirs(output_dir, exist_ok=True)

    #Cria um cliente S3 com credenciais explícitas
    session = boto3.Session(
        aws_access_key_id= AWS_ACCESS_KEY_ID,
        aws_secret_access_key= AWS_SECRET_ACCESS_KEY,
        region_name= AWS_REGION
    )

    s3 = session.client('s3')

    try:
        # Lista todos os buckets
        response = s3.list_buckets()
        print('Buckets disponíveis:')
        for bucket in response['Buckets']:
            print(f'  {bucket["Name"]}')
    except Exception as e:
        print(f"Erro ao listar buckets: {e}")
        pass
        

    df = pd.read_csv(csv_file)
    print(df)

    json_file = os.path.join(output_dir, "MovieAndTv.json")
    df.to_json(json_file, orient='records', lines=False)

    parquet_file = os.path.join(output_dir, 'MovieAndTv.parquet')
    table = pa.Table.from_pandas(df)
    pq.write_table(table, parquet_file)

    s3.upload_file(parquet_file, bucket_name, s3_key)
    
