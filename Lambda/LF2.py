import json
import boto3
import requests
import random
import logging
from requests_aws4auth import AWS4Auth
from boto3.dynamodb.conditions import Attr

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

region = 'us-east-1'
service = 'es'
HOST = "https://search-dining-concierge-jddhnyt5wuyuzj4gzsqpqbhwni.us-east-1.es.amazonaws.com/"
SQSURL = "https://sqs.us-east-1.amazonaws.com/863570438364/DiningConciergeSQS"
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
RECOMMENDATIONMAX = 3
CHARSET = "UTF-8"


def verify_email(email):
    ses_client = boto3.client('ses', region_name= 'us-east-1')
    status = ses_client.get_identity_verification_attributes(Identities=[email],)
    if not status['VerificationAttributes'] or \
            status['VerificationAttributes'][email]['VerificationStatus']=='Failed' or \
            status['VerificationAttributes'][email]['VerificationStatus']=='TemporaryFailure' or \
            status['VerificationAttributes'][email]['VerificationStatus']=='NotStarted':
        ses_client.verify_email_identity(EmailAddress=email)
        return "Failed"
    else:
        return status['VerificationAttributes'][email]['VerificationStatus']


def send_email(email, msg):
    ses_client = boto3.client('ses', region_name= 'us-east-1')
    try:
        ses_client.send_email(
            Destination = {
                "ToAddresses": [email],
            },
            Message = {
                "Body": {
                    "Text": {
                        "Charset": CHARSET,
                        "Data": msg,
                    }
                },
                "Subject": {
                    "Charset": CHARSET,
                    "Data": "Your Dining Suggestion",
                },
            },
            Source="mh6069@nyu.edu",
        )
        return True
    except:
        return False


def poll_message():
    sqs_client = boto3.client('sqs')
    response = sqs_client.receive_message(
        QueueUrl = SQSURL,
        MessageAttributeNames = ['All'],
        VisibilityTimeout = 0,
        WaitTimeSeconds = 0
    )
    if "Messages" in response:
        message = response["Messages"][0]
        return message
        
        
def delete_message(ReceiptHandle):
    sqs_client = boto3.client('sqs')
    sqs_client.delete_message(
        QueueUrl = SQSURL,
        ReceiptHandle = ReceiptHandle
    )
    
    
def pull_info(resturant_id):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('yelp-restaurants')
    try:
        response = table.scan(FilterExpression=Attr('id').eq(resturant_id))
        info = response["Items"]
        while('LastEvaluatedKey' in response):
            LastEvaluatedKey = response['LastEvaluatedKey']
            response = table.scan(FilterExpression=Attr('id').eq(resturant_id), ExclusiveStartKey=LastEvaluatedKey)
            info += response["Items"]
        info = info[0]
    except IndexError:
        logger.debug("Error pull restaurant {} info from dynamodb".format(resturant_id))
        return
    return info
    

def lambda_handler(event, context):
    message = poll_message()
    if message:
        cuisine = message['MessageAttributes']['Cuisine']['StringValue']
        location = message['MessageAttributes']['Location']['StringValue']
        number_of_people = message['MessageAttributes']['NumberOfPeople']['StringValue']
        date = message['MessageAttributes']['Date']['StringValue']
        time = message['MessageAttributes']['Time']['StringValue']
        phone = message['MessageAttributes']['Phone']['StringValue']
        email = message['MessageAttributes']['Email']['StringValue']
        
        email_status = verify_email(email)
        if email_status == 'Failed':
            return {'statusCode': 200,'body': "Email not verified"}
        elif email_status == 'Pending':
            return {'statusCode': 200,'body': "Waiting for email verification"}
            
        msg = "Hello! Here are my {} restaurant suggestions for {} people, for {} at {}: ".format(cuisine, number_of_people, date, time)
        
        try:
            count_req = requests.get(HOST+'_count?q={}'.format(cuisine), auth=awsauth)
            count = int(json.loads(count_req.text)['count'])
        except KeyError:
            logger.debug("Error when extracting count")
            return
        
        if count > 0:
            number_of_recommedation = min(count, RECOMMENDATIONMAX)
            random_idx = random.sample(range(0, count), number_of_recommedation)
            for i in range(number_of_recommedation):
                try:
                    req = requests.get(HOST+'_search?q={}&from={}&size=1'.format(cuisine, random_idx[i]), auth=awsauth)
                    restaurant = json.loads(req.text)['hits']['hits'][0]
                except KeyError:
                    logger.debug("Error getting restaurant from OpenSearch")
                    return 
                info = pull_info(restaurant['_id'])
                if not info: return
                msg = msg + "\r\n{idx}. {name}, located at {addr}. ".format(idx=i+1, name=info['name'], addr=info['address'])
            msg = msg + "\r\n\r\nEnjoy your meal!"
        else:
            msg = "Sorry, I don't have any recommedation for {cuisine} restaurant at {loc} for {num} people.".format(cuisine=cuisine, loc=location, num=number_of_people)
        
        if not send_email(email, msg): return
    
        sns_client = boto3.client('sns', region_name= 'us-east-1')
        if phone and phone[0]!='+':
            phone = '+1'+phone
        sns_client.publish(PhoneNumber=phone, Message=msg)
        
        delete_message(message['ReceiptHandle'])
            
        return {
            'statusCode': 200,
            'body': "LF2 done with message: {}".format(message['MessageId'])
        }
        
    else:
        return {
            'statusCode': 200,
            'body': "No message in SQS"
        }