import pandas as pd     # For opening and creating dataframes
import numpy as np      # For creating mathematical and array operations
import boto3            # For using AWS services in python
import json             # Converting from Python to JSON format or vice-versa

# Class to segregate data whether it is Samsung or Fitbit and return new dataframe with required values
class SegregateData:
    def __init__(self):
        # Initialising an empty dataframe with required headers
        self.__fitbit_headers = ['Id', 'Time', 'Value']
        self.__samsung_headers = ['heart_rate', 'max', 'start_time', 'end_time', 'create_time', 'update_time', 'min', 'uuid']
    
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

    def __add_to_sqs(self, values):
        try:
            # Create SQS client
            sqs_client = boto3.client("sqs",endpoint_url = "http://localhost:4566")
            message = values
            # Sending message to SQS queue
            response = sqs_client.send_message(
                QueueUrl="http://localhost:4566/000000000000/queue",
                MessageBody=json.dumps(message)
            )
        except Exception as e:
            print('Exception:',e)

    def __add_to_dataframe_fitbit(self,dataframe):
        values = {
            'timestamp': list(dataframe['Time']),
            'user_id': list(dataframe['Id']),
            'heart_rate': list(dataframe['Value']),
            'vendor': 'Fitbit'
        }
        self.__add_to_sqs(values)
            
    def __add_to_dataframe_samsung(self,dataframe):
        values = {
            'timestamp': list(dataframe['create_time']),
            'user_id': list(dataframe['uuid']),
            'heart_rate': list(dataframe['heart_rate']),
            'vendor':'Samsung'
        }
        self.__add_to_sqs(values)
        
    # Function to fit the file to required dataframe
    def fit(self,filepath):
        try:
            file_dataframe = pd.read_csv(filepath)     # Reading csv file from the given file path
            is_fitbit = self.__is_fitbit(file_dataframe) # Checking whether data belongs to fitbit or not
            if is_fitbit:
                self.__add_to_dataframe_fitbit(file_dataframe)
            else:
                is_samsung = self.__is_samsung(file_dataframe)
                if is_samsung:
                    self.__add_to_dataframe_samsung(file_dataframe)
        except Exception as e:
            print('Exception:', e)

    # Function to return final modified dataframe with required values
    def dataframe(self):
        return self.__dataframe

dataframes = []
file_path = ['samsung_streaming.csv','fitbit_streaming.csv']
for path in file_path:    
    obj = SegregateData()
    obj.fit(filepath = path)