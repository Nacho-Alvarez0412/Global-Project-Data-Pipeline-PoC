from flask import Flask,request
from flask_restful import Api


from Producer import TransactionProducer
from Consumer import TransactionConsumer


app = Flask(__name__)
api = Api(app)
producer = TransactionProducer()
consumer = TransactionConsumer()

@app.route("/transaction/api/stop",methods=['GET'])
def stop_consumption():
    consumer.stop_consumption()
    return f'Daemon stopped consuming transactions',200

@app.route("/transaction/api/start",methods=['GET'])
def start_consumption():
    if(consumer.begin_consumption()):
        return f'Daemon resume consuming transactions',200
    else:
        return f'Daemon is already running',200
    
@app.route("/transaction/verify",methods=['POST'])
def verifyTransaction():
    transaction = request.get_json()
    if transaction:
        producer.verify_transaction(transaction,transaction["transactionId"])
        transaction = consumer.await_classification(transaction["transactionId"])
        
        if(transaction['approved']):
            return f'Transaction {transaction} approved',200
        else:
            return f'Transaction declined',401
    else: 
        return 'Transaction couldn´t be processed', 400  


if __name__ ==  "__main__":
    app.run(port=5000)

