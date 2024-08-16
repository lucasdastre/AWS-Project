import boto3

# Crie uma sessão com suas credenciais
session = boto3.Session(
    aws_access_key_id='SUA_ACCESS_KEY',
    aws_secret_access_key='SUA_SECRET_KEY',
    region_name='us-east-1'  # ou outra região apropriada
)

# Crie um cliente para S3
s3_client = session.client('s3')

try:
    response = s3_client.list_buckets()
    buckets = response['Buckets']
    for bucket in buckets:
        print(bucket['Name'])
except Exception as e:
    print(f'Erro ao listar buckets: {e}')
