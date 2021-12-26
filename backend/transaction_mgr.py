import os
import json
import uuid
import datetime
import boto3
from collections import defaultdict
from decimal import Decimal
from botocore.exceptions import ClientError

ddb = boto3.resource('dynamodb')
user_table_name = os.environ.get('USER_TABLE')
group_table_name = os.environ.get('GROUP_TABLE')
trans_table_name = os.environ.get('TRANS_TABLE')

try:
    user_table = ddb.Table(user_table_name)
    group_table = ddb.Table(group_table_name)
    trans_table = ddb.Table(trans_table_name)
except Exception as e:
    print(f"error initializing table connection - {e}")


def lambda_handler(event :dict, context):
    print(f"incoming event - {json.dumps(event)}")
    http_method = event.get('httpMethod')

    if http_method == 'POST':
        return add_new_transaction(event)
    else:
        return response(405, {'error':'method not allowed'})


def add_new_transaction(event):
    try:
        request_body = json.loads(event.get('body'))
    except Exception as e:
        print(f"Error request body not valid json - {event.get('body')} | {e}")
        return response(400, {'error': 'invalid parameters'})

    # validate request params
    if 'name' not in request_body or 'total_amount' not in request_body or 'group_id' not in request_body or \
    'participants' not in request_body or type(request_body['participants']) is not list or \
    'payers' not in request_body or type(request_body['payers']) is not dict:
        return response(400, {'error': 'invalid parameters'})

    transaction_id = uuid.uuid4().hex
    timestamp = datetime.datetime.now().isoformat()
    participants = request_body['participants']
    payers = request_body['payers']

    if not validate_group(request_body['group_id']):
        return response(400, {'error': 'invalid group'})

    if not validate_users(participants, request_body['group_id']):
        return response(400, {'error': 'invalid participants list'})
    if not validate_users(payers.keys(), request_body['group_id']):
        return response(400, {'error': 'invalid payers list'})

    payers_sum = 0
    for key in payers:
        payers_sum += payers[key]
    if payers_sum != request_body['total_amount']:
        return response(400, {'error': 'total_amount mismatch'})
    
    if not add_trans_to_group(request_body['group_id'], transaction_id):
        return response(400, {'error': 'group_id invalid'})

    resolved_payables = calculate_balances(request_body['total_amount'], payers, participants)
    # Float to deciman conv for ddb
    ddb_resolved_payables = json.loads(json.dumps(resolved_payables), parse_float=Decimal)

    try:
        trans_table.put_item(
            Item={
                'trans_id': transaction_id,
                'name': request_body['name'],
                'trans_date': timestamp,
                'payers': payers,
                'participants': participants,
                'total_amount': request_body['total_amount'],
                'group_id': request_body['group_id'],
                'payables': ddb_resolved_payables,
                'details': request_body.get('details', '')
            }
        )
    except Exception as e:
        print(f"Error in adding new transaction {request_body['name']}, trans_id = {transaction_id}. Error - {e}")
        return response(500, {'error': 'error adding new transaction'})

    return response(200, {'status': 'success', 'message': f'transaction {request_body["name"]}', 'trans_id': transaction_id})


def calculate_balances(total_amount, payers, participants):
    per_person = (total_amount / len(participants))
    payables = {}
    for person in participants:
        payables[person] = -per_person
    for person in payers:
        payables[person] = payers[person] - per_person
    print(f"calculating balances - {payables}")
    return payables


def add_trans_to_group(groupid, transactionid):
    try:
        group_table.update_item(
                Key={
                    'group_id': groupid
                },
                UpdateExpression="SET #g = list_append(#g, :transid)",
                ExpressionAttributeNames={
                    "#g": "transactions",
                },
                ExpressionAttributeValues={
                    ":transid": [transactionid]
                },
                ConditionExpression=boto3.dynamodb.conditions.Attr("group_id").exists()
            )
    except ClientError as err:
        if err.response["Error"]["Code"] == 'ConditionalCheckFailedException':
            # user_id does not exist
            print(f"group_id - {groupid} not found")

    except Exception as e:
        print(f"Error occured in updating user table for {groupid} | {e}")
        return False
    return True


def validate_group(groupid):
    try:
        ret = group_table.get_item(
            Key = {
                'group_id': groupid
            }
        )
        if 'Item' not in ret:
            return False
    except Exception as e:
        print(f"Error in validating group_id {groupid} | {e}")
        return False
    return True

def validate_users(users, groupid):
    try:
        ret = group_table.get_item(
            Key = {
                'group_id': groupid
            }
        )
        if 'Item' not in ret:
            return False
    except Exception as e:
        print(f"Error in reading db for group_id {groupid} | {e}")
        return False
    
    group_users = ret['Item']['members']
    for user_id in users:
        if user_id not in group_users:
            print(f"{user_id} not in {group_users}")
            return False
    return True

def response(err_code :int, body :dict):
    return {
        'statusCode': err_code,
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps(body)
    }