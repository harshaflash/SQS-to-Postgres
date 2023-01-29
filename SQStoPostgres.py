import psycopg2
import json
from datetime import datetime
import hashlib
import pandas as pd

AWS_REGION = 'us-east-1'
AWS_PROFILE = 'localstack'
ENDPOINT_URL = "http://localhost:4566"

import boto3
boto3.setup_default_session(profile_name=AWS_PROFILE)
sqs = boto3.client('sqs',region_name='us-east-1',endpoint_url = ENDPOINT_URL)
queue_url= 'http://localhost:4566/000000000000/login-queue'

conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="postgres")

cur = conn.cursor()
print('PostgreSQL database version:')
cur.execute('SELECT version()')
db_version = cur.fetchone()
print(db_version)
alter_sql = """Alter table user_logins alter column app_version type varchar(32)"""
cur.execute(alter_sql)
conn.commit()

cur.execute("""
    SELECT column_name, data_type, character_maximum_length
    FROM information_schema.columns
    WHERE table_name = 'user_logins'
""")
print(cur.fetchall())



def mask_fields(data, fields_to_mask):
    for field in fields_to_mask:
        if field in data:
            # Generate a hash of the original value
            hashed_value = hashlib.sha256(data[field].encode()).hexdigest()
            # Keep the first 8 characters of the hash
            data[field] = hashed_value[:8]
    return data


c=2
while c>1:
    response = sqs.receive_message(QueueUrl=queue_url,AttributeNames=['SentTimestamp'],
    MaxNumberOfMessages=1,
    VisibilityTimeout=0,
    WaitTimeSeconds=10
    )
    if 'Messages' in response:
        messages = response['Messages']

        for message in messages:
            receiptHandle = message['ReceiptHandle']
            body = json.loads(message['Body'])
            print(receiptHandle)
            print(body)
            print(mask_fields(body,['device_id', 'ip']))
            if 'user_id' in body.keys():
                value = (body['user_id'],body['device_type'],body['ip'],body['device_id'],body['locale'],body['app_version'],str(datetime.now()))
                sql = f"INSERT INTO user_logins (user_id,device_type,masked_ip,masked_device_id,locale,app_version,create_date) VALUES (%s,%s,%s,%s,%s,%s,%s)"
                print(sql,value)
                try:
                    cur.execute(sql,value)
                    conn.commit()
                    sqs.delete_message(QueueUrl=queue_url,ReceiptHandle=receiptHandle)
                except(Exception, psycopg2.DatabaseError) as error:
                    print(error)
            else:
                print('Invalid data')
                sqs.delete_message(QueueUrl=queue_url,ReceiptHandle=receiptHandle)
    else:
        c-=1


cur.execute("select * from user_logins")
data = cur.fetchall()
df = pd.DataFrame(data, columns =['user_id','device_type','masked_ip','masked_device_id','locale','app_version','create_date'])
df.shape