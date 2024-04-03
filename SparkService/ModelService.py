import requests
import json

url = "http://127.0.0.1:5002"

def postTransaction(transaction):
    response = requests.post(url=url+"/model/classify",json=transaction,headers={"Content-Type": "application/json"})
    if response.status_code == 200:
        print(f"Success! transaction id:{transaction['transactionId']} sent successfully.")
    else:
        print(f"Error: {response.status_code}")
        # Access the error message if available
        error_content = response.content.decode()
        print(f"Error details: {error_content}")