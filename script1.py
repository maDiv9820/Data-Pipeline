import library as lb

# Class to segregate data whether it is Samsung or Fitbit and return new dataframe with required values
class SegregateData:
    def __init__(self):
        # Initialising an empty dataframe with required headers
        self.__fitbit_headers = lb.fitbit_headers
        self.__samsung_headers = lb.samsung_headers
        
        # For Mapping
        self.__fitbit_dict = {}
        self.__samsung_dict = {}
        
        # Creating a kinesis client for sending the message
        self.__kinesis_client = lb.boto3.client('kinesis', endpoint_url = lb.endpoint_url)
    
    def __is_fitbit(self,dataframe):
        # As from the sample fitbit, there are 3 columns in the fit, so we can assume that the given file
        # can be of fitbit device
        colcount = 0
        for fitcol in self.__fitbit_headers:
            for col in dataframe.columns:
                if fitcol.lower() in col.lower():
                    colcount = colcount+1
                    self.__fitbit_dict[fitcol.lower()] = col
                    break

        if colcount != len(dataframe.columns):
            self.__fitbit_dict = {}
            return False
        
        return True

    def __is_samsung(self,dataframe):
        # Checking whether dataframe has same columns as in samsung sample file
        colcount = 0
        for samcol in self.__samsung_headers:
            for col in dataframe.columns:
                if samcol.lower() in col.lower():
                    colcount = colcount+1
                    self.__samsung_dict[samcol.lower()] = col
                    break

        if colcount != len(dataframe.columns):
            self.__samsung_dict = {}
            return False

        return True

    # Function to add message to kinesis stream
    def __add_to_kinesis(self, message):
        try:
            response = self.__kinesis_client.put_record(
                StreamName = lb.stream_name,
                Data = lb.json.dumps(message),
                PartitionKey = 'Partition_Key'
            )
            print(response)
        except Exception as e:
            lb.logging.error(f'Error: {e}')
    
    # ---------------------- Creating message for Queue in the required data format ---------------------------------- #

    def __create_msg_fitbit(self,dictionary):
        values = {}
        for col in lb.data_headers[:-1]:
            fitbit_col = lb.data_fitbit_dict[col]
            values[col] = dictionary[self.__fitbit_dict[fitbit_col]]
        values[lb.data_headers[-1]] = 'Fitbit'
        lb.logging.info(f'Message Created Successfully')
        self.__add_to_kinesis(values)
            
    def __create_msg_samsung(self,dictionary):
        values = {}
        for col in lb.data_headers[:-1]:
            samsung_col = lb.data_samsung_dict[col]
            values[col] = dictionary[self.__samsung_dict[samsung_col]]
        values[lb.data_headers[-1]] = 'Samsung'
        lb.logging.info(f'Message Created Successfully')
        self.__add_to_kinesis(values)
    
    # ---------------------------------------------------------------------------------------------------------------- #

    # For streaming the data
    def __stream_data(self, dataframe, isfit = False, issamsung = False):
        for row in dataframe.values:        # Fetching each row from the Dataframe
            values = {}
            for index in range(len(dataframe.columns)):     # Creating a dictionary for each row
                values[dataframe.columns[index]] = row[index]
            if isfit:
                self.__create_msg_fitbit(values)
            elif issamsung:
                self.__create_msg_samsung(values)

        
    # Function to fit the file to required dataframe
    def fit(self,filepath):
        try:
            file_dataframe = lb.pd.read_csv(filepath)       # Reading csv file from the given file path
            if self.__is_fitbit(file_dataframe):            # Checking whether data belongs to fitbit or not
                self.__stream_data(file_dataframe, isfit = True)
            elif self.__is_samsung(file_dataframe):         # Checking whether data belongs to samsung or not
                self.__stream_data(file_dataframe, issamsung = True)
        except Exception as e:
            lb.logging.error(f'Error: {e}')

# Main Code
if __name__ == '__main__':
    try:
        for path in lb.file_paths:    
            obj = SegregateData()
            obj.fit(filepath = path)
        lb.logging.info(f'Process Completed successfully')
    except Exception as e:
        lb.logging.error(f'Error: {e}')