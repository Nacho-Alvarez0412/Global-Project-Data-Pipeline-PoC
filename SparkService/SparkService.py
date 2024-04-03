from flask import Flask,request
from flask_restful import Api

from Consumer import TransactionConsumer


app = Flask(__name__)
api = Api(app)
consumer = TransactionConsumer()



@app.route("/spark/api/stop",methods=['GET'])
def stop_consumption():
    consumer.stop_consumption()
    return f'Daemon stopped consuming transactions',200

@app.route("/spark/api/start",methods=['GET'])
def start_consumption():
    if(consumer.begin_consumption()):
        return f'Daemon resume consuming transactions',200
    else:
        return f'Daemon is already running',200


if __name__ ==  "__main__":
    app.run(port=5001)
    



