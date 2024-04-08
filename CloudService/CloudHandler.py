import psycopg2

def lst2pgarr(alist):
    return '{' + ','.join(str(x) for x in alist) + '}'

class CloudHandler:
    connection = None
    cursor = None


    def connect(self):
        self.connection = psycopg2.connect(
            database="TransactionHistory",
            user="postgres",
            password="admin",
            host="localhost",
            port="5432",
        )
        self.cursor = self.connection.cursor()
        
    def insertTransaction(self,transaction):
        self.connect()
        insert_query = f"""INSERT INTO transactions(transactionid,approved,transactiondetails) VALUES ('{transaction["transactionId"]}',{transaction["approved"]},'{lst2pgarr(transaction["transaction"][0])}')"""
        self.cursor.execute(insert_query)
        self.connection.commit()
        self.cursor.close()
        self.connection.close()
        


