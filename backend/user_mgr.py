import os
import json
import uuid
import datetime
import boto3

ddb = boto3.resource('dynamodb')
user_table_name = os.environ.get('USER_TABLE')

try:
    user_table = ddb.Table(user_table_name)
except Exception as e:
    print(f"error initializing table connection {user_table_name} - {e}")


def lambda_handler(event :dict, context):
    print(f"incoming event - {json.dumps(event)}")
    http_method = event.get('httpMethod')

    if http_method == 'POST':
        return add_new_user(event)
    elif http_method == 'GET':
        return ret_user_details(event)
    else:
        return response(405, {'error':'method not allowed'})


def add_new_user(event):
    try:
        request_body = json.loads(event.get('body'))
    except Exception as e:
        print(f"Error request body not valid json - {event.get('body')} | {e}")
        return response(400, {'error': 'invalid parameters'})

    # validate request params
    if 'name' not in request_body or 'email' not in request_body:
        return response(400, {'error': 'invalid parameters'})

    user_id = uuid.uuid4().hex[:8]
    timestamp = datetime.datetime.now().isoformat()

    try:
        user_table.put_item(
            Item={
                'user_id': user_id,
                'join_date': timestamp,
                'groups': [],
                'name': request_body['name'],
                'email': request_body['email']
            }    
        )
    except Exception as e:
        print(f"Error in adding new user {request_body['name']}, user_id = {user_id}. Error - {e}")
        return response(500, {'error': 'error adding new user'})
    
    return response(200, {'status': 'success', 'message': f'user {request_body["name"]}', 'user_id': user_id})


def ret_user_details(event):
    path_params = event.get('pathParameters')
    if 'user_id' not in path_params:
        return response(400, {'error': 'bad request'})
    user_id = path_params['user_id']
    try:
        ret = user_table.get_item(
            Key={
                'user_id': user_id
            }
        )
    except Exception as e:
        print(f"Error in fetching ddb entry for key(user_id) = {user_id}")
        return response(500, {'error': 'unable to find details'})
    
    if 'Item' not in ret:
        return response(400, {'message': 'provided user_id not found'})
    
    return response(200, {
        'status': 'success',
        'name': ret['Item']['name'],
        'email': ret['Item']['email'],
        'groups': ret['Item']['groups']
    })


def response(err_code :int, body :dict):
    return {
        'statusCode': err_code,
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps(body)
    }