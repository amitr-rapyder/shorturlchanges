import boto3
import json
import os
import uuid
import re
from time import time

region = os.environ['Region']
app_url = os.environ['APP_URL']
table_name = os.environ['TABLE_NAME']
ddb = boto3.client('dynamodb')

def create_short_url(url_list):
    converted_urls = []
    
    for long_url in url_list:
        short_id = str(uuid.uuid4())[0:8]
        
        # Check if ID already exists and regenerate if needed
        while 'Item' in ddb.get_item(
            Key={'short_id': {'S': short_id}},
            TableName=table_name
        ):
            short_id = str(uuid.uuid4())[0:8]
            
        short_url = f"{app_url}/{short_id}"
        ttl_value = int(time()) + 604800  # 7 days expiry
        
        # Store in DynamoDB
        ddb.put_item(
            TableName=table_name,
            Item={
                'short_id': {'S': short_id},
                'created_at': {'S': str(time())},
                'short_url': {'S': short_url},
                'long_url': {'S': long_url},
                'ttl_value': {'S': str(ttl_value)}
            }
        )
        
        converted_urls.append({long_url: short_url})
    
    return converted_urls

def extract_urls(message):
    regex = r"(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'\".,<>?«»]))"
    urls = re.findall(regex, message)
    return [x[0] for x in urls]

def lambda_handler(event, context):
    try:
        body = json.loads(event['body'])
        long_url = body['longUrl']
        
        # Extract and validate URLs
        url_list = extract_urls(long_url)
        if not url_list:
            return {
                'statusCode': 400,
                'body': 'No valid URL found, please enter a valid URL'
            }
        
        # Create short URLs
        converted_urls = create_short_url(url_list)
        short_url = list(converted_urls[0].values())[0]  # Get first shortened URL
        
        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'Your short url is': short_url}),
            'isBase64Encoded': False
        }
        
    except Exception as e:
        return {
            'statusCode': 400,
            'body': str(e)
        }