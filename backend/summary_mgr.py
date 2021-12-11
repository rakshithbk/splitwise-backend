import os
import json
import boto3
from collections import defaultdict
from decimal import Decimal
from botocore.exceptions import ClientError

class DecimalEncoder(json.JSONEncoder):
  def default(self, obj):
    if isinstance(obj, Decimal):
      return str(obj)
    return json.JSONEncoder.default(self, obj)

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

    if http_method == 'GET':
        return computed_transaction(event)
    else:
        return response(405, {'error':'method not allowed'})


def computed_transaction(event):
    path_params = event.get('pathParameters')
    if 'group_id' not in path_params:
        return response(400, {'error': 'bad request'})

    group_id = path_params['group_id']

    # get all transactions for that groupid
    transaction_ids = get_transaction_ids(group_id)
    if transaction_ids is None:
        return response(500, {'error': 'unable to find details for provided group_id'})

    # get transactions from table
    transactions_list = get_transactions(transaction_ids)
    if transactions_list is None:
        return response(500, {'error': 'unable to resolve transactions for provided group_id'})

    # compute payables and return
    consolidated_payables = defaultdict(dict)
    for trans in transactions_list:
        for receiver in trans:
            for payer in trans[receiver]:
                consolidated_payables[receiver][payer] = consolidated_payables[receiver].get(payer, 0) + trans[receiver][payer]
    print(f"consolidated payable - {consolidated_payables}")
    return response(200, dict(consolidated_payables))


def get_transactions(trans_ids):
    transactions = []
    for transid in trans_ids:
        try:
            ret = trans_table.get_item(
                Key={
                    'trans_id': transid
                }
            )
        except Exception as e:
            print(f"Error in fetching ddb entry for key(trans_id) = {transid}")
            return None
        
        if 'Item' not in ret:
            return None
        transactions.append(ret['Item']['payables'])
    return transactions


def get_transaction_ids(groupid):
    try:
        ret = group_table.get_item(
            Key={
                'group_id': groupid
            }
        )
    except Exception as e:
        print(f"Error in fetching ddb entry for key(group_id) = {groupid}")
        return None
    
    if 'Item' not in ret:
        return None
    return ret['Item']['transactions']


def response(err_code :int, body :dict):
    return {
        'statusCode': err_code,
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps(body, cls=DecimalEncoder)
    }