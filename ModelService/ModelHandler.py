from tensorflow.keras.models import load_model
import random
import pickle
import os
import re

os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'



class ModelHandler:
    model = None

    def __init__(self,model):
        self.load_model(model)
        
    def classify_transaction(self,transaction):
        print(f'Procesando transacción id {transaction["transactionId"]}')
        transaction["approved"] = self.predict_transaction(transaction["transaction"])
        print(f'Transacción {transaction["transactionId"]}: {transaction["approved"]}')
        return transaction
    
    def predict_transaction(self,transaction):
        if(self.model.predict(transaction)[0]): # Positive fraud case is 1
            return False
        else:
            return True

    def dummy_classification(self,transaction):
        transaction["approved"] = random.randint(0,100) > 25
        return transaction
    
    def load_model(self,filename):
        # Debido a la complejidad de las arquitecturas CNN es necesario usar la función propia de TF para guardar y cargar el modelo
        relative_path = f"\ModelService\Models\{filename}"
        current_dir = os.getcwd()  
        absolute_path = current_dir + relative_path
        if(re.findall(".pkl", absolute_path)):
            with open(absolute_path, 'rb') as f:
                self.model = pickle.load(f)
        else:
            self.model = load_model(absolute_path)