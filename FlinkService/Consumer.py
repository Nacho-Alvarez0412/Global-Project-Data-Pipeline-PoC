import sys
sys.path.append('Kafka')
from config import config
from confluent_kafka import Consumer, KafkaException
import json
import threading
from ModelService import postTransaction
import csv
import os
import joblib
import pandas as pd



def getAbsolutePath(relative_path):
    current_dir = os.getcwd()  
    return current_dir + relative_path


class TransactionConsumer:
    consumer = None
    consume = True
    consumer_thread = None
    scaler = None
    
    def __init__(self):
        self.lock = threading.Lock()
        self.begin_consumption()
        self.scaler = joblib.load(getAbsolutePath('\FlinkService\StandardScaler.fit'))
        
    def begin_consumption(self):
        if self.consumer_thread and self.consumer_thread.is_alive():
            return False
        
        self.set_consumer_configs()
        self.consumer = Consumer(config)
        self.consumer.subscribe(['Raw_Transactions'], on_assign=self.assignment_callback)
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
        config['group.id'] = 'Raw_Transactions_ETL'
        config['auto.offset.reset'] = 'earliest'
        config['enable.auto.commit'] = False
        
    def pass_transaction(self,transactionId,transaction):
        postTransaction(transactionId,transaction)
        
    def process_transaction(self,transaction):
        processed_transaction = self.get_transaction_data(int(transaction["transactionId"]))
        return self.scaler.transform(processed_transaction) 
    
    def get_transaction_data(self,line_number):
        try:
            relative_path = "\FlinkService\creditcard_2023.csv"
            current_dir = os.getcwd()  
            absolute_path = current_dir + relative_path
            line_number += 2 # Offset
            with open(absolute_path, 'r') as csvfile:
                reader = csv.reader(csvfile, delimiter=',')
                line_count = 0
                headers = []
                selectedRow = []
                for row in reader:
                    if line_count == 0:
                        headers = row
                    line_count += 1
                    if line_count == line_number:
                        selectedRow = row
                        break;
                df = pd.DataFrame([selectedRow], columns=headers)
                df.set_index('id', inplace = True)
                return df.drop(columns = ['Class'], axis=1)
        except FileNotFoundError:
            raise FileNotFoundError("CSV file  not found.")
        
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
                    processed_transaction = self.process_transaction(transaction)
                    self.pass_transaction(transaction["transactionId"],processed_transaction)
        except KeyboardInterrupt:
            print('Canceled by user.')
        finally:
            self.consumer.close()
    
    
    
    
    
    
    