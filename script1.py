import pandas as pd     # For opening and creating dataframes
import numpy as np

# Class to segregate data whether it is Samsung or Fitbit and return new dataframe with required values
class SegregateData:
    def __init__(self):
        # Initialising an empty dataframe with required headers
        self.__dataframe = pd.DataFrame([], columns = ['timestamp','user_id','heart_rate','vendor'])
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

    def __add_to_dataframe_fitbit(self,dataframe):
        values = {
            'timestamp': dataframe['Time'],
            'user_id': dataframe['Id'],
            'heart_rate': dataframe['Value'],
            'vendor': 'Fitbit'
        }
        temp_df = pd.DataFrame(values)
        self.__dataframe = pd.concat([self.__dataframe,temp_df], ignore_index = True)
    
    def __add_to_dataframe_samsung(self,dataframe):
        values = {
            'timestamp':dataframe['create_time'],
            'user_id':dataframe['uuid'],
            'heart_rate':dataframe['heart_rate'],
            'vendor':'Samsung'
        }
        temp_df = pd.DataFrame(values)
        self.__dataframe = pd.concat([self.__dataframe,temp_df], ignore_index = True)

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
file_path = ['/Users/manish.mathur/Downloads/Assignment/Samsung-Fitbit Data/heartrate_seconds_merged.csv',
             '/Users/manish.mathur/Downloads/Assignment/Samsung-Fitbit Data/samsung_data_final.csv']
for path in file_path:    
    obj = SegregateData()
    obj.fit(filepath = path)
    dataframe = obj.dataframe()
    dataframes.append(dataframe)
final_dataframe = pd.concat(dataframes, ignore_index =  True)
vendors = np.unique(final_dataframe['vendor'])
print(vendors)
final_dataframe.to_csv('Final_Data.csv', index = False)