import os
import json
import uuid
import datetime
import boto3
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
        return add_new_group(event)
    elif http_method == 'GET':
        return ret_group_details(event)
    else:
        return response(405, {'error':'method not allowed'})


def add_new_group(event):
    try:
        request_body = json.loads(event.get('body'))
    except Exception as e:
        print(f"Error request body not valid json - {event.get('body')} | {e}")
        return response(400, {'error': 'invalid parameters'})

    # validate request params
    if 'name' not in request_body or 'members' not in request_body or type(request_body['members']) is not list:
        return response(400, {'error': 'invalid parameters'})

    group_id = uuid.uuid4().hex[:8]
    timestamp = datetime.datetime.now().isoformat()
    members = request_body['members']

    validated_members = update_user_table(members, group_id)
    
    if validated_members is None:
        return response(500, {'error': 'error validating members'})
    
    if len(validated_members) == 0:
        return response(400, {'error': 'member ids passed are not valid'})

    try:
        group_table.put_item(
            Item={
                'group_id': group_id,
                'name': request_body['name'],
                'join_date': timestamp,
                'members': validated_members,
                'transactions': [],
                'details': request_body.get('details', '')
            }    
        )
    except Exception as e:
        print(f"Error in adding new group {request_body['name']}, group_id = {group_id}. Error - {e}")
        return response(500, {'error': 'error adding new user'})
    
    return response(200, {'status': 'success', 'message': f'group {request_body["name"]}', 'group_id': group_id})


def update_user_table(members, group_id):
    validated_members = []
    for user_id in members:
        try:
            ret = user_table.update_item(
                Key={
                    'user_id': user_id
                },
                UpdateExpression="SET #g = list_append(#g, :groupid)",
                ExpressionAttributeNames={
                    "#g": "groups",
                },
                ExpressionAttributeValues={
                    ":groupid": [group_id]
                },
                ConditionExpression=boto3.dynamodb.conditions.Attr("user_id").exists()
            )
        
        except ClientError as err:
            if err.response["Error"]["Code"] == 'ConditionalCheckFailedException':
                # user_id does not exist
                continue

        except Exception as e:
            print(f"Error occured in updating user table for {user_id} | {e}")
            return None

        validated_members.append(user_id)
    return validated_members        


def ret_group_details(event):
    path_params = event.get('pathParameters')
    if 'group_id' not in path_params:
        return response(400, {'error': 'bad request'})
    group_id = path_params['group_id']
    try:
        ret = group_table.get_item(
            Key={
                'group_id': group_id
            }
        )
    except Exception as e:
        print(f"Error in fetching ddb entry for key(group_id) = {group_id} | {e}")
        return response(500, {'error': 'unable to find details'})
    
    if 'Item' not in ret:
        return response(400, {'message': 'provided group_id not found'})
    
    return response(200, {
        'status': 'success',
        'name': ret['Item']['name'],
        'members': ret['Item']['members'],
        'transactions': ret['Item']['transactions']
    })


def response(err_code :int, body :dict):
    return {
        'statusCode': err_code,
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps(body)
    }