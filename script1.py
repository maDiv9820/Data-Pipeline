import library as lb

# Class to segregate data whether it is Samsung or Fitbit and return new dataframe with required values
class SegregateData:
    def __init__(self):
        # Initialising an empty dataframe with required headers
        self.__fitbit_headers = lb.fitbit_headers
        self.__samsung_headers = lb.samsung_headers
    
    def __is_fitbit(self,dataframe):
        # As from the sample fitbit, there are 3 columns in the fit, so we can assume that the given file
        # can be of fitbit device
        if list(dataframe.columns) == self.__fitbit_headers:
            return True
        return False

    def __is_samsung(self,dataframe):
        # Checking whether dataframe has same columns as in samsung sample file
        if list(dataframe.columns) == self.__samsung_headers:
            return True
        return False

    def __add_to_sqs(self, message):
        try:
            # Create SQS client
            sqs_client = lb.boto3.client("sqs", endpoint_url = lb.endpoint_url)
            # Sending message to SQS queue
            response = sqs_client.send_message(
                QueueUrl = lb.queue_url,
                MessageBody = lb.json.dumps(message)
            )
            lb.logging.info(f'Added to Queue Successfully')
        except Exception as e:
            lb.logging.error(f'Error: {e}')

    def __create_msg_fitbit(self,dataframe):
        values = {
            'timestamp': list(dataframe['Time']),
            'user_id': list(dataframe['Id']),
            'heart_rate': list(dataframe['Value']),
            'vendor': 'Fitbit'
        }
        lb.logging.info(f'Message Created Successfully')
        self.__add_to_sqs(values)
            
    def __create_msg_samsung(self,dataframe):
        values = {
            'timestamp': list(dataframe['create_time']),
            'user_id': list(dataframe['uuid']),
            'heart_rate': list(dataframe['heart_rate']),
            'vendor':'Samsung'
        }
        lb.logging.info(f'Message Created Successfully')
        self.__add_to_sqs(values)
        
    # Function to fit the file to required dataframe
    def fit(self,filepath):
        try:
            file_dataframe = lb.pd.read_csv(filepath)     # Reading csv file from the given file path
            is_fitbit = self.__is_fitbit(file_dataframe) # Checking whether data belongs to fitbit or not
            if is_fitbit:
                self.__create_msg_fitbit(file_dataframe)
            else:
                is_samsung = self.__is_samsung(file_dataframe)
                if is_samsung:
                    self.__create_msg_samsung(file_dataframe)
        except Exception as e:
            lb.logging.error(f'Error: {e}')

# Main Code
if __name__ == '__main__':
    try:
        file_paths = lb.file_paths
        for path in file_paths:    
            obj = SegregateData()
            obj.fit(filepath = path)
        lb.logging.info(f'Process Completed successfully')
    except Exception as e:
        lb.logging.error(f'Error: {e}')