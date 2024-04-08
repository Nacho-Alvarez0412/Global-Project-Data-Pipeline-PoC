from flask import Flask,request
from flask_restful import Api
import time


from Producer import TransactionProducer
from Consumer import TransactionConsumer


app = Flask(__name__)
api = Api(app)
producer = TransactionProducer()
consumer = TransactionConsumer()

@app.route("/transaction/api/stop",methods=['GET'])
def stop_consumption():
    consumer.stop_consumption()
    return 'Daemon stopped consuming transactions',200

@app.route("/transaction/api/start",methods=['GET'])
def start_consumption():
    if(consumer.begin_consumption()):
        return 'Daemon resume consuming transactions',200
    else:
        return 'Daemon is already running',200
    
@app.route("/transaction/verify",methods=['POST'])
def verify_transaction():
    start_time = time.perf_counter()
    transaction = request.get_json()
    if transaction:
        producer.verify_transaction(transaction,transaction["transactionId"])
        transaction = consumer.await_classification(transaction["transactionId"])
        end_time = time.perf_counter()
        total_time = end_time - start_time
        print(f'Elapsed time for transaction id: {transaction["transactionId"]} was {round(total_time,2)} seconds')
        if(transaction['approved']):
            return f'Transaction {transaction["transactionId"]} approved',200
        else:
            return 'Transaction declined',401
    else: 
        return 'Transaction couldn´t be processed', 400  


if __name__ ==  "__main__":
    app.run(port=5000)

