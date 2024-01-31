from dotenv import load_dotenv
import requests
import json
import pika
import os

load_dotenv()
api_key = os.getenv('COINMARKETCAP_API_KEY')
headers = {
    'Accepts': 'application/json',
    'X-CMC_PRO_API_KEY': api_key,
}
url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'

try:
    # API request
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()

        # Setup RabbitMQ connection and channel
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            host='localhost',
            credentials=pika.PlainCredentials('rmq','guest')
        ))
        channel = connection.channel()
        print(connection)
        print(channel)

        # Declare queue
        channel.queue_declare(queue='crypto_data')

        # Send message
        channel.basic_publish(exchange='', routing_key='crypto_data', body=json.dumps(data))
    else:
        print(f"Failed to fetch data: Status code {response.status_code}")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    # Clean up and close connections
    if 'channel' in locals():
        channel.close()
    if 'connection' in locals():
        connection.close()