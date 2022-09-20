import sqlite3      # For creating a database and dumping data into the table
import boto3        # For using AWS Services
import pandas as pd # For creating dataframes

class SQSToDB:
    def start(self):
        self.__recieve_message()

    def __recieve_message(self):
        while True:
            try:
                # Create SQS client
                sqs_client = boto3.client("sqs",endpoint_url = "http://localhost:4566")
                # Receive message from SQS queue
                response = sqs_client.receive_message(
                    QueueUrl = "http://localhost:4566/000000000000/queue",
                    AttributeNames = ['timestamp','user_id','heart_rate','vendor'],   # Attributes in the body of the message
                    MaxNumberOfMessages = 1,
                    MessageAttributeNames = ['All'],
                    VisibilityTimeout = 0,
                    WaitTimeSeconds = 0
                )
                message = response['Messages'][0]
                receipt_handle = message['ReceiptHandle']
                body = message['Body']
                self.__saveToDB(body)   # Saving to the database

                # Delete received message from queue
                sqs_client.delete_message(
                    QueueUrl = "http://localhost:4566/000000000000/queue",
                    ReceiptHandle = receipt_handle
                )

            except Exception as e:
                break
    
    # Function to save the data into the database
    def __saveToDB(self,values):
        connector = sqlite3.connect('database.sqlite')  # Connecting with the database
        df =  pd.DataFrame(eval(values))    # Creating a dataframe of the given values
        df.to_sql('heart_rate', connector, if_exists = 'append', index = False) # Dumping into the table

obj = SQSToDB()
obj.start()