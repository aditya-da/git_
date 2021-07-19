from kafka import KafkaConsumer
from mysql.connector import errorcode
import mysql.connector
from datetime import datetime
import time
import json,os
import redis

redisHost = os.environ['REDIS_HOST']
redisPort = os.environ['REDIS_PORT']

redis_client = redis.Redis(host=redisHost,port=redisPort,db=0)

con = mysql.connector.connect(user=os.environ['MYSQL_USER'], password=os.environ['MYSQL_PASSWORD'],
                              host=os.environ['MYSQL_HOST'],database=os.environ['MYSQL_DB'])
cursor=con.cursor()

consumer = KafkaConsumer('payment.transaction', bootstrap_servers=['172.31.37.90:9092','172.31.43.186:9092','172.31.41.240:9092'],
                         auto_offset_reset='latest', enable_auto_commit=True,
                         auto_commit_interval_ms=1000, group_id='merchnat-product-consumer')


for message in consumer:
    print(message)
    if(len(message.value.decode('utf-8'))>0):
        message = json.loads(message.value.decode('utf-8'))
        print(message['id'], message['createdAt'], message['updatedAt'])

       
        if 'merchantId' in message.keys() and message['status'] == 'ACTIVE' :
            
            created_at = datetime.strptime(message['createdAt'], '%Y-%m-%d %H:%M:%S')
            updated_at = datetime.strptime(message['updatedAt'], '%Y-%m-%d %H:%M:%S')
          
            query = "INSERT INTO merchant_product (id,merchant_id,fp_account_active,fp_account_status, created_at, updated_at) VALUES (%s,%s, %s, %s, %s ,%s) ON DUPLICATE KEY UPDATE id = VALUES(id),merchant_id = VALUES(merchant_id),fp_account_active = VALUES(fp_account_active),fp_account_status = VALUES(fp_account_status),created_at = VALUES(created_at),updated_at = VALUES(updated_at);"
            cursor.execute(query,(message['id'],message['merchantId'],1,message['status'],created_at,updated_at))
            con.commit()
          
            redis_client.delete('__main__.merchant_product(body=merchant_id={},client=recommend)'.format(message['merchantId']))
         
        
        elif 'merchantId' in message.keys() and message['status'] != 'ACTIVE':
            
            created_at = datetime.strptime(message['createdAt'], '%Y-%m-%d %H:%M:%S')
            updated_at = datetime.strptime(message['updatedAt'], '%Y-%m-%d %H:%M:%S')
            
            query = "INSERT INTO merchant_product (id,merchant_id,fp_account_active,fp_account_status, created_at, updated_at) VALUES (%s,%s, %s, %s, %s ,%s) ON DUPLICATE KEY UPDATE id = VALUES(id),merchant_id = VALUES(merchant_id),fp_account_active = VALUES(fp_account_active),fp_account_status = VALUES(fp_account_status),created_at = VALUES(created_at),updated_at = VALUES(updated_at);"
            cursor.execute(query,(message['id'],message['merchantId'],0,message['status'],created_at,updated_at))
            con.commit()
          
            redis_client.delete('__main__.merchant_product(body=merchant_id={},client=recommend)'.format(message['merchantId']))
            
        else:
            pass
        

cursor.close()
con.close()