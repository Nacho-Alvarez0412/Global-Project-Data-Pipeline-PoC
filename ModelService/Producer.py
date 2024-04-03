import sys
sys.path.append('Kafka')
from config import config
from confluent_kafka import Producer
import json


class TransactionProducer:
    producer = Producer(config)

    def callback(self,err, event):
        if err:
            print(f'Produce to topic {event.topic()} failed for event: {event.key()}')
        else:
            val = event.value().decode('utf8')
            print(f'{val} sent to partition {event.partition()}')
            
    def publish_transaction_classification(self, transaction,key):
        self.producer.produce('Classified_Transactions', json.dumps(transaction), key, on_delivery=self.callback)
        self.producer.flush()
    