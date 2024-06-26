import sys
sys.path.append('Kafka')
from config import config
from confluent_kafka import Consumer, KafkaException
import json
import threading
from CloudHandler import CloudHandler




class TransactionConsumer:
    consumer = None
    consume = True
    consumer_thread = None
    cloudHandler = None

    
    def __init__(self):
        self.lock = threading.Lock()
        self.begin_consumption()
        self.cloudHandler = CloudHandler()

        
    def begin_consumption(self):
        if self.consumer_thread and self.consumer_thread.is_alive():
            return False
        
        self.set_consumer_configs()
        self.consumer = Consumer(config)
        self.consumer.subscribe(['Classified_Transactions'], on_assign=self.assignment_callback)
        self.consumer_thread = threading.Thread(target=self.consume_transactions,daemon=True)
        with self.lock:
            self.consume = True
            self.consumer_thread.start()
        return True
    
    def stop_consumption(self):
        with self.lock:
            self.consume = False
            print('Stop classification consumption')
        return True
    
    def assignment_callback(self,consumer, partitions):
        for p in partitions:
            print(f'Awaiting for {p.topic} in partition {p.partition}')

    def set_consumer_configs(self):
        config['group.id'] = 'Classified_Transactions_Historic'
        config['auto.offset.reset'] = 'earliest'
        config['enable.auto.commit'] = False
        
    def save_transaction(self,transaction):
        self.cloudHandler.insertTransaction(transaction)
        
    def consume_transactions(self):
        try:    
            while self.consume:
                event = self.consumer.poll(10)
                if event is None:
                    continue
                if event.error():
                    raise KafkaException(event.error())
                else:
                    transaction = json.loads(event.value().decode('utf8'))
                    partition = event.partition()
                    print(f'Received: {transaction} from partition {partition}')
                    self.consumer.commit(event)
                    self.save_transaction(transaction)
        except KeyboardInterrupt:
            print('Canceled by user.')
        finally:
            self.consumer.close()
    
    
    
    
    
    
    