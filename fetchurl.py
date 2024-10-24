import os
import boto3
from urllib.parse import urlparse
from time import time

ddb = boto3.client('dynamodb')

def add(true_long_url):
    parsed = urlparse(true_long_url)
    if not parsed.scheme:
        return f'http://{true_long_url}'
    return true_long_url

def counter(table_name, short_id, hits):
    return ddb.update_item(
        TableName=table_name,
        Key={'short_id': {'S': short_id}},
        UpdateExpression='SET hits = :h, hit_time = :ht',
        ExpressionAttributeValues={
            ':h': {'N': str(hits)},
            ':ht': {'N': str(time())}
        }
    )

def lambda_handler(event, context):
    try:
        short_id = event["short_id"]
        
        rep = ddb.get_item(
            Key={'short_id': {'S': short_id}},
            TableName=os.environ['TABLE_NAME']
        )
        
        true_long_url = add(rep['Item']['long_url']['S'])
        
        hits = 0
        if 'hits' in rep['Item']:
            hits = int(rep['Item']['hits']['N'])
        hits += 1
        
        response_update = counter(
            os.environ['TABLE_NAME'], 
            short_id, 
            hits
        )
        
        return {
            'statusCode': 301,
            'location': true_long_url
        }
        
    except Exception as e:
        return {
            'statusCode': 301,
            'location': json.dumps({"error": "Not able to find URL"})
        }