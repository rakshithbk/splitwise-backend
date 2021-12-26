import os
import json
import boto3
from copy import deepcopy
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
    user_amounts = {}

    for trans in transactions_list:
        for party in trans:
            user_amounts[party] = user_amounts.get(party, 0) + trans[party]

    settlements = simplify_settlements(user_amounts)
    if settlements is None:
        return response(500, {'error': 'transaction amounts mismatch, double entry transactions does not add to zero'})
    return response(200, settlements)


def simplify_settlements(final_amounts):
    total = 0
    for user in final_amounts:
        total += final_amounts[user]
    if abs(total) >= 0.1:
        print(f"final total amounts does not add up to 0 ({total}). mismatch - {final_amounts}")
        return None
    
    consolidated_payables = defaultdict(dict)
    min_cash_flow(final_amounts, consolidated_payables)
    detailed_list = detailed_settlement_list(consolidated_payables)
    
    settlements = dict(consolidated_payables)
    settlements['details'] = detailed_list
    return settlements


def get_min_usr(amounts):
    
    minusr = next(iter(amounts))
    for user in amounts:
        if (amounts[user] < amounts[minusr]):
            minusr = user
    return minusr


def get_max_usr(amounts):

    maxusr = next(iter(amounts))
    for user in amounts:
        if (amounts[user] > amounts[maxusr]):
            maxusr = user
    return maxusr


def min_cash_flow(amount, final_settle):

    max_creditor = get_max_usr(amount)
    min_debitor = get_min_usr(amount)

    # If both amounts are 0 (or near, due to float precision), then all amounts are settled
    if (abs(amount[max_creditor]) <= 0.1 and abs(amount[min_debitor]) <= 0.1):
        return 0

    min_amount = min(-amount[min_debitor], amount[max_creditor])
    amount[max_creditor] -=min_amount
    amount[min_debitor] += min_amount

    # store the settlement details in defaultdict(dict)
    final_settle[max_creditor][min_debitor] = round(min_amount, 2)
    final_settle[min_debitor][max_creditor] = round(-min_amount, 2)

    min_cash_flow(amount, final_settle)


def detailed_settlement_list(consolidated_payables):
    user_ids = []
    user_id_name = {}
    for user in consolidated_payables:
        user_ids.append(user)
    
    for userid in user_ids:
        try:
            ret = user_table.get_item(
                Key={
                    'user_id' : userid
                }
            )
        except Exception as e:
            print(f"Exception in fetching username for {userid} - {e}")

        if 'Item' in ret:
            user_id_name[userid] = ret['Item']['name']
        else:
            print(f"error user id not found - {userid}")
            user_id_name[userid] = "<unknown>"
    
    details = []
    payables = deepcopy(consolidated_payables)
    for parties in payables:
        for payer in payables[parties]:
            if payables[parties][payer] > 0:
                details.append(f"{user_id_name[payer]} should pay Rs.{payables[parties][payer]} to {user_id_name[parties]}")
                payables[payer].pop(parties)
    return details


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