import json
import boto3
import logging

def lambda_handler(event, context):
    client = boto3.client('lexv2-runtime')
    
    response = client.recognize_text(
        botId='YEOQVWAGBZ',
        botAliasId='TSTALIASID',
        localeId='en_US',
        sessionId=event["uid"],
        text=event["messages"])
        
    return {
        'statusCode': 200,
        'body': response,
        'messages': response['messages'][0]['content'],
        'headers': {
            'Access-Control-Allow-Headers' : 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
    }