import json
import random


class ModelHandler:
    model = 'Modelo acá'

    def __init__(self):
        self.model = 'Setup del modelo acá'
        
    def classify_transaction(self,transaction):
        print(f'Procesando transacción id {transaction["transactionId"]}')
        classified_transaction = self.dummyClassification(transaction) # Acá se procesa
        print(f'Transacción {transaction["transactionId"]}: {classified_transaction["approved"]}')
        return classified_transaction
    
    def dummyClassification(self,transaction):
        transaction["approved"] = random.randint(0,100) > 25
        return transaction
        