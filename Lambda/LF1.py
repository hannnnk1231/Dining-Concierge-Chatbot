import math
import dateutil.parser
import datetime
import time
import os
import logging
import boto3
from boto3.dynamodb.conditions import Attr


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def push_message(intent_request):
    client = boto3.client('sqs')
    
    location = get_slots(intent_request)["Location"]['value']['resolvedValues'][0]
    cuisine = get_slots(intent_request)["Cuisine"]['value']['resolvedValues'][0]
    number_of_people = get_slots(intent_request)["NumberOfPeople"]['value']['resolvedValues'][0]
    date = get_slots(intent_request)["Date"]['value']['resolvedValues'][0]
    time = get_slots(intent_request)["Time"]['value']['resolvedValues'][0]
    phone = get_slots(intent_request)["PhoneNumber"]['value']['resolvedValues'][0]
    email = get_slots(intent_request)["Email"]['value']['resolvedValues'][0]
    
    
    response = client.send_message(
        QueueUrl = "https://sqs.us-east-1.amazonaws.com/863570438364/DiningConciergeSQS",
        MessageBody = "Message from LF1",
        MessageAttributes = {
            "Location": {
                "StringValue": location,
                "DataType": "String"
            },
            "Cuisine": {
                "StringValue": cuisine,
                "DataType": "String"
            },
            "NumberOfPeople": {
                "StringValue": number_of_people,
                "DataType": "Number"
            },
            "Date": {
                "StringValue": date,
                "DataType": "String"
            },
            "Time": {
                "StringValue": time,
                "DataType": "String"
            },
            "Phone": {
                "StringValue": phone,
                "DataType": "String"
            },
            "Email": {
                "StringValue": email,
                "DataType": "String"
            }
        }
        )
        
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('dining_user')
    table.put_item(Item = {'uid':intent_request['sessionId'], "cuisine":cuisine, "location":location})
    
def get_slots(intent_request):
    return intent_request['sessionState']['intent']['slots']


def elicit_slot(intent_name, slots, slot_to_elicit, message):
    return {
        "sessionState":{
            'dialogAction': {
                'type': 'ElicitSlot',
                
                'slotToElicit': slot_to_elicit
            },
            "intent":{
                'name': intent_name,
                'slots': slots
            }
        },
        'messages': [message]
    }


def close(intent_name, fulfillment_state, message):
    response = {
        "sessionState":{
            'dialogAction': {
                'type': 'Close',
            },
        
            "intent":{
                "name": intent_name,
                "state": fulfillment_state
            },
        },
        'messages': [message]
    }

    return response


def delegate(intent_name, slots):
    return {
        "sessionState":{
            'dialogAction': {
                'type': 'Delegate',
            },
            "intent": {
                "state": "ReadyForFulfillment",
                "name": intent_name,
                'slots': slots
            }
        }
    }
    

def confirm_intent(intent_name, slots, message):
    return {
        "sessionState": {
            'dialogAction': {
                'type': "ConfirmIntent",
            },
            'intent': {
                "name": intent_name,
                'slots': slots
            }
        },
        'messages': [{
            'contentType': 'PlainText',
            'content': message
        }]
    }

def parse_int(n):
    try:
        return int(n)
    except ValueError:
        return float('nan')


def build_validation_result(is_valid, violated_slot, message_content):
    if message_content is None:
        return {
            "isValid": is_valid,
            "violatedSlot": violated_slot,
        }

    return {
        'isValid': is_valid,
        'violatedSlot': violated_slot,
        'message': {'contentType': 'PlainText', 'content': message_content}
    }


def isvalid_date(date):
    try:
        dateutil.parser.parse(date)
        return True
    except ValueError:
        return False

def validate_dining_suggestion(location, cuisine, number_of_people, date, time, email):
    
    if location is not None:
        if not location['value']['resolvedValues']:
            return build_validation_result(False,'Location','Please provide a valid city such as NYC or Boston')
    
    cuisine_types = ["American", "Chinese", "Indian", "Italian", "Korean"]
    if cuisine is not None:
        if not cuisine['value']['resolvedValues'] or cuisine['value']['resolvedValues'][0] not in cuisine_types:
            return build_validation_result(False,'Cuisine','Please select a cuisine style from following: American, Chinese, Indian, Italian, Korean')
    
    if number_of_people is not None:
        if not number_of_people['value']['resolvedValues']:
            return build_validation_result(False, 'NumberOfPeople', "Please provide a valid number such as 2 or 10")
        num = parse_int(number_of_people['value']['resolvedValues'][0])
        if math.isnan(num):
            return build_validation_result(False, 'NumberOfPeople', 'Please enter a valid number.')
        if num <= 0 or num > 20:
            return build_validation_result(False, 'NumberOfPeople', 'I can suggest a restaurant from 1 person to 20 people. Can you specify a number in this range?')

    if date is not None:
        if not date['value']['resolvedValues']:
            return build_validation_result(False, 'Date', "Please provide a valid date such as today or 9/30")
        date = date['value']['resolvedValues'][0]
        if not isvalid_date(date):
            return build_validation_result(False, 'Date', 'I did not understand that, what date would you like to dine?')
        elif datetime.datetime.strptime(date, '%Y-%m-%d').date() < datetime.date.today():
            return build_validation_result(False, 'Date', 'I can suggest a restaurant for you from today.  What day would you like to reserve?')

    if time is not None:
        if not time['value']['resolvedValues']:
            return build_validation_result(False, 'Time', "Please provide a valid time such as 6pm or 18:00")
        time = time['value']['resolvedValues'][0]
        if len(time) != 5:
            return build_validation_result(False, 'Time', None)

        hour, minute = time.split(':')
        hour = parse_int(hour)
        minute = parse_int(minute)
        if math.isnan(hour) or math.isnan(minute):
            return build_validation_result(False, 'Time', None)
        current_time = datetime.datetime.now()
        current_hour, current_minute = current_time.hour, current_time.minute
        if datetime.datetime.strptime(date, '%Y-%m-%d').date() == datetime.date.today() and hour <= current_hour and minute <= current_minute:
            # Lesser than the current time
            return build_validation_result(False, 'Time', 'Can you specify a time that greater than the current time ({}:{})?'.format(current_hour,current_minute))
            
    if email is not None:
        if not email['value']['resolvedValues']:
            return build_validation_result(False, "Email", 'Please provide a valid email.')
        email = email['value']['resolvedValues'][0]
        if '@' not in email:
            return build_validation_result(False, "Email", 'Your email address misses an "@". Please provide a valid email.')

    return build_validation_result(True, None, None)


def dining_suggestion(intent_name, intent_request):
    """
    Performs dialog management and fulfillment for dining suggestion.
    Beyond fulfillment, the implementation of this intent demonstrates the use of the elicitSlot dialog action
    in slot validation and re-prompting.
    """
    
    location = get_slots(intent_request)["Location"]
    cuisine = get_slots(intent_request)["Cuisine"]
    number_of_people = get_slots(intent_request)["NumberOfPeople"]
    date = get_slots(intent_request)["Date"]
    time = get_slots(intent_request)["Time"]
    phone = get_slots(intent_request)["PhoneNumber"]
    email = get_slots(intent_request)["Email"]
    
    source = intent_request['invocationSource']

    if source == 'DialogCodeHook':
        # Perform basic validation on the supplied input slots.
        # Use the elicitSlot dialog action to re-prompt for the first violation detected.
        slots = get_slots(intent_request)

        validation_result = validate_dining_suggestion(location, cuisine, number_of_people, date, time, email)
        if not validation_result['isValid']:
            slots[validation_result['violatedSlot']] = None
            return elicit_slot(
                intent_name,
                slots,
                validation_result['violatedSlot'],
                validation_result['message']
            )

        return delegate(intent_name, slots)

    # Send out the suggestions, and rely on the goodbye message of the bot to define the message to the end user.
    push_message(intent_request)
    return close(
        intent_name,
        'Fulfilled',
        {
            'contentType': 'PlainText',
            'content': "You're all set. I will sent the {} style {} restaurant suggestions for {} people, for {} at {} to your phone: {} and your e-mail: {} shortly. Have a nice day.".format(cuisine['value']['resolvedValues'][0], location['value']['resolvedValues'][0], number_of_people['value']['resolvedValues'][0], date['value']['resolvedValues'][0], time['value']['resolvedValues'][0], phone['value']['resolvedValues'][0], email['value']['resolvedValues'][0])
            
        }
    )


def return_user(intent_name, intent_request):
    slots = get_slots(intent_request)
    source = intent_request['invocationSource']
    userid = intent_request['sessionId']
    if source == "DialogCodeHook":
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('dining_user')
        uid = intent_request['sessionId']
        response = table.scan(FilterExpression=Attr('uid').eq(uid))
        info = response["Items"]
        if info:
            info = info[0]
            location, cuisine = info['location'], info['cuisine']
            slots['Cuisine'] = {'value': {
                  "originalValue": cuisine,
                  "interpretedValue": cuisine,
                  "resolvedValues": [cuisine]
            }}
            slots['Location'] = {'value': {
                  "originalValue": location,
                  "interpretedValue": location,
                  "resolvedValues": [location]
            }}
            msg = "Hi, you ask for {Location}'s {Cuisine} style restaurant last time. Do you want to continue with the same cuisine style and the same location?".format(Location=location, Cuisine=cuisine)
            return confirm_intent("DiningSuggestionIntent", slots, msg)
        else:
            return close(intent_name, 'Fulfilled', {'contentType':'PlainText', 'content':'Hi there, how can I help?'})

def dispatch(intent_request):
    intent_name = intent_request['sessionState']['intent']['name']
    
    if intent_request['sessionState']['intent']['confirmationState']=='Denied':
        return close(
            intent_name,
            'Fulfilled',
            {
                'contentType': 'PlainText',
                'content': 'Well, how can I help?'
            }
        )
    
    elif intent_name == 'DiningSuggestionIntent':
        return dining_suggestion(intent_name, intent_request)
    elif intent_name == 'GreetingIntent':
        return close(
            intent_name,
            'Fulfilled',
            {
                'contentType': 'PlainText',
                'content': 'Hi there, how can I help?'
            }
        )
    elif intent_name == 'ThankYouIntent':
        return close(
            intent_name,
            'Fulfilled',
            {
                'contentType': 'PlainText',
                'content': "You're welcome, have a nice day."
            }
        )
    elif intent_name == "ReturnUserIntent":
        return return_user(intent_name, intent_request)
    
    raise Exception('Intent with name ' + intent_name + ' not supported')


def lambda_handler(event, context):
    """
    Route the incoming request based on intent.
    The JSON body of the request is provided in the event slot.
    """
    # By default, treat the user request as coming from the America/New_York time zone.
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    logger.debug('event.bot.name={}'.format(event['bot']['name']))

    return dispatch(event)
