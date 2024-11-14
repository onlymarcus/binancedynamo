import json
import boto3
from botocore.exceptions import ClientError
from binance.client import Client
from binance.streams import BinanceSocketManager
from datetime import datetime
from decimal import Decimal
from dynamodb_config import create_trade_table, save_trade_data
import logging
import asyncio
import websockets.exceptions

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Inicialize o cliente da Binance
api_key = "YOUR_BINANCE_API_KEY"
api_secret = "YOUR_BINANCE_API_SECRET"
client = Client(api_key, api_secret)

# Cria a tabela se não existir
logger.info("Verificando se a tabela do DynamoDB existe ou precisa ser criada.")
create_trade_table()

# Função para processar os dados de trade
def process_message(msg):
    if msg['e'] == 'trade':
        # Extraia os dados relevantes do trade
        trade_data = {
            'pair': msg['s'],  # Par de moedas
            'trade_id': str(msg['t']),
            'timestamp': str(datetime.fromtimestamp(msg['T'] / 1000.0)),
            'buyer_is_maker': msg['m'],
            'quantity': Decimal(msg['q']),
            'price': Decimal(msg['p'])
        }

        # Salve os dados no DynamoDB
        logger.info(f"Processando trade: {trade_data}")
        save_trade_data(trade_data)

# Função principal assíncrona para iniciar o WebSocket com tentativa de reconexão
async def main():
    while True:
        try:
            logger.info("Inicializando o gerenciador de WebSocket da Binance.")
            bsm = BinanceSocketManager(client)
            
            # Inicialize o socket para o par de moedas desejado
            symbol = 'btcusdt'
            logger.info(f"Iniciando o WebSocket para o par de moedas: {symbol}")
            
            # Utilize o método correto para iniciar o WebSocket
            socket = bsm.trade_socket(symbol)
            
            # Inicie o loop para processar mensagens
            async with socket as stream:
                while True:
                    msg = await stream.recv()
                    process_message(msg)
        except (websockets.exceptions.ConnectionClosed, asyncio.CancelledError) as e:
            logger.error(f"Conexão perdida: {e}. Tentando reconectar em 5 segundos...")
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"Erro inesperado: {e}. Tentando reconectar em 5 segundos...")
            await asyncio.sleep(5)

# Execute o loop principal
if __name__ == "__main__":
    logger.info("Iniciando o loop principal do WebSocket.")
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())



