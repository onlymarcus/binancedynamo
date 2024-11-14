import boto3
from botocore.exceptions import ClientError
from decimal import Decimal

# Inicialize o cliente do DynamoDB
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

# Nome da tabela no DynamoDB
table_name = "TradeData"
table = dynamodb.Table(table_name)

# Função para criar a tabela do DynamoDB, se não existir
def create_trade_table():
    try:
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {'AttributeName': 'trade_id', 'KeyType': 'HASH'},  # Chave primária
            ],
            AttributeDefinitions=[
                {'AttributeName': 'trade_id', 'AttributeType': 'S'},
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        # Aguarda a criação da tabela
        table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
        print(f"Tabela {table_name} criada com sucesso.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            print(f"Tabela {table_name} já existe.")
        else:
            print(f"Erro ao criar tabela: {e}")

# Função para salvar os dados de trade no DynamoDB
def save_trade_data(trade_data):
    try:
        # Converter valores numéricos para Decimal, se necessário
        for key, value in trade_data.items():
            if isinstance(value, float):
                trade_data[key] = Decimal(str(value))

        table.put_item(Item=trade_data)
        print(f"Dados inseridos: {trade_data}")
    except Exception as e:
        print(f"Erro ao inserir dados: {e}")
