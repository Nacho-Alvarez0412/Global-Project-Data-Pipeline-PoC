from flask import Flask,request
from flask_restful import Api
from Producer import TransactionProducer
from ModelHandler import ModelHandler


app = Flask(__name__)
api = Api(app)
producer = TransactionProducer()
model_handler = ModelHandler('RandomForest.pkl')

@app.route("/model/classify",methods=['POST'])
def classify_transaction():
    transaction = request.get_json()
    if transaction:
        classified_transaction = model_handler.classify_transaction(transaction)
        producer.publish_transaction_classification(classified_transaction,classified_transaction["transactionId"])
    return 'Transactions received',200

if __name__ ==  "__main__":
    app.run(port=5002)
    



