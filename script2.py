import library as lb
class SQSToDB:
    def start(self):
        self.__recieve_message()

    def __recieve_message(self):
        while True:
            try:
                # Create SQS client
                sqs_client = lb.boto3.client("sqs", endpoint_url = lb.endpoint_url)
                # Receive message from SQS queue
                response = sqs_client.receive_message(
                    QueueUrl = lb.queue_url,
                    AttributeNames = lb.data_headers,   # Attributes in the body of the message
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
                    QueueUrl = lb.queue_url,
                    ReceiptHandle = receipt_handle
                )

            except Exception as e:
                print('Exception:', e)
                break
    
    # Function to save the data into the database
    def __saveToDB(self,values):
        connector = lb.sqlite3.connect(lb.database_name)  # Connecting with the database
        df =  lb.pd.DataFrame(eval(values))    # Creating a dataframe of the given values
        df.to_sql(lb.table_name, connector, if_exists = 'append', index = False) # Dumping into the table

obj = SQSToDB()
obj.start()