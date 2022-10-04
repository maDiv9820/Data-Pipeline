import library as lb
class KinesisToDB:
    def __init__(self):
        self.__kinesis_client = lb.boto3.client('kinesis', endpoint_url = lb.endpoint_url)
    
    # Start function 
    def start(self):
        self.__receive_messages()

    def __receive_messages(self):
        try:
            stream_description = self.__kinesis_client.describe_stream(StreamName = lb.stream_name)
            shard_id = stream_description['StreamDescription']['Shards'][0]['ShardId']
            shard_iterator = self.__kinesis_client.get_shard_iterator(
                StreamName = lb.stream_name,
                ShardId = shard_id,
                ShardIteratorType = 'LATEST'
            )['ShardIterator']

            while shard_iterator is not None:
                result = self.__kinesis_client.get_records(ShardIterator=shard_iterator)
                shard_iterator = result['NextShardIterator']
                records = result["Records"]
                for count in range(len(records)):
                    encoded_data = records[count]['Data'].decode('utf-8')
                    data = lb.json.loads(encoded_data)
                    self.__saveToDB(data)
        except Exception as e:
            lb.logging.error(f'Error: {e}')
    
    # Function to save the data into the database
    def __saveToDB(self, message):
        connector = lb.sqlite3.connect(lb.database_name)  # Connecting with the database
        temp_df = lb.pd.DataFrame([list(message.values())], columns = list(message.keys()))
        temp_df.to_sql(lb.table_name, connector, if_exists = 'append', index = False) # Dumping into the table
        lb.logging.info(f'All data dump to the database successfully')

# Main Code
if __name__ == '__main__':
    try:
        obj = KinesisToDB()
        obj.start()
        lb.logging.info(f'Process Completed successfully')
    except Exception as e:
        lb.logging.error(f'Error: {e}')