import library as lb
class SQSToDB:
    def __init__(self):
        self.__messages = []    # Saving all the messages from Queue
        self.__sqs_client = lb.boto3.client("sqs", endpoint_url = lb.endpoint_url)
    
    # Start function 
    def start(self):
        self.__recieve_messages()
        self.__saveToDB()
        self.__delete_messages()

    # Receiving the messages from SQS queue
    def __recieve_messages(self):
        # while True:
        for counter in range(3):
            try:
                # Create SQS client
                # Receive message from SQS queue
                response = self.__sqs_client.receive_message(
                    QueueUrl = lb.queue_url,
                    AttributeNames = lb.data_headers,   # Attributes in the body of the message
                    MaxNumberOfMessages = 10,
                    MessageAttributeNames = ['All'],
                    VisibilityTimeout = 2,
                    WaitTimeSeconds = 0
                )
                self.__messages = response['Messages']
            except Exception as e:
                break
    
    
    # Function to save the data into the database
    def __saveToDB(self):
        if len(self.__messages) > 0:
            connector = lb.sqlite3.connect(lb.database_name)  # Connecting with the database
            df =  lb.pd.DataFrame([], columns = lb.data_headers)   # Creating a dataframe of the given values
            for message in self.__messages:
                temp_df = lb.pd.DataFrame(eval(message['Body']))
                df = lb.pd.concat([df,temp_df], ignore_index = True)
            df.to_sql(lb.table_name, connector, if_exists = 'append', index = False) # Dumping into the table

    def __delete_messages(self):
        if len(self.__messages) > 0:
            for message in self.__messages:
                receipt_handle = message['ReceiptHandle']
                response = self.__sqs_client.delete_message(
                    QueueUrl = lb.queue_url,
                    ReceiptHandle = receipt_handle
                )
            self.__messages = []     # Since we have deleted from Queue, we can delete from object too

try:
    obj = SQSToDB()
    obj.start()
except Exception as e:
    print('Exception:', e)