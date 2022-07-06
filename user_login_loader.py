import json
import gzip
import localstack_client.session as boto3
import psycopg2 as psy
from datetime import date
import pandas as pd

QUEUE_NAME = "login-queue"

# method to extract data from sqs messages.
def get_message():
    #initializing boto3 and response object
    sqs_client = boto3.client("sqs")
    response = sqs_client.receive_message (
        QueueUrl = "awslocal sqs receive-message --queue-url http://localhost:4566/000000000000/login-queue",
        MaxNumberOfMessages = 100,
        WaitTimeSeconds = 10,
    )
    
    # list to store the records
    data = []
    
    # extracting records from messages into the data list
    for message in response.get("Messages", []):
        message_body = message["Body"]
        data.append(json.loads(message_body))
        
    return data

# storing data in json file format in data variable
print("getting data from SQS queue")
data = get_message()

# creating a datafram of the data we extracted from sqs queue
df = pd.DataFrame(data)

# this is the mask dictionary which will mask the numeric characters in PII data with special characters
mask = {0: '=', 1: '%', 2: '&', 3: '*', 4: '-', 5: '@', 6: '!', 7: '$', 8: '#', 9: '+'}

# method to apply mask to PII data
# parses the dataframe and every string in the column name provided
# to replace numeric characters with special characters
def mask_col(col_name):
    for i in range(len(df)):
        char_list = []
        for char in df.at[i, col_name]:
            if char.isdigit():
                num = int(char)
                char_list.append(mask[num])
            else:
                char_list.append(char)
        df.at[i, col_name] = ''.join(char_list)

# Masking the ip and device_id columns
print("Masking PII data")
mask_col('ip')
mask_col('device_id')

# Method which makes connection with postgres using the psycopg2 library
# then loads the masked data into the user_login table
def insert_data():
    conn = psy.connect("dbname='postgres' user='postgres' password='postgres' host=localhost")
    cur = conn.cursor()
    for i in range(len(df)):
        cur.execute("INSERT INTO user_logins VALUES(%s, %s, %s, %s, %s, %s, %s)",
                    [df.at[i, 'user_id'], df.at[i, 'device_type'], df.at[i, 'ip'],
                    df.at[i, 'device_id'], df.at[i, 'locale'],
                    int(df.at[i, 'app_version'].replace('.', '')), date.today()])
    conn.commit()
    conn.close()

# running the load
print("inserting data into user_login")
insert_data()
print("complete!")