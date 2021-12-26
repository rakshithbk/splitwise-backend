from aws_cdk import (
    core as cdk,
    aws_apigateway as apigateway,
    aws_lambda as lambda_,
    aws_dynamodb as ddb
)

class InfraStack(cdk.Stack):

    def __init__(self, scope: cdk.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # The code that defines your stack goes here

        # DDB table
        user_table = ddb.Table(
            self, "registered_users",
            table_name="splitwise_registered_users",
            partition_key=ddb.Attribute(
                name='user_id', 
                type=ddb.AttributeType.STRING
            )
        )

        transactions_table = ddb.Table(
            self, "transactions",
            table_name="splitwise_transactions",
            partition_key=ddb.Attribute(
                name='trans_id', 
                type=ddb.AttributeType.STRING
            )
        )

        groups_table = ddb.Table(
            self, "user_groups",
            table_name="splitwise_user_groups",
            partition_key=ddb.Attribute(
                name='group_id', 
                type=ddb.AttributeType.STRING
            )
        )

        # Create Lambda handlers
        create_user_lambda = lambda_.Function(
            self, "create_user_func",
            function_name= "splitwise_create_user_func",
            runtime=lambda_.Runtime.PYTHON_3_8,
            code=lambda_.Code.from_asset('../backend'),
            handler="user_mgr.lambda_handler",
            environment={
                'USER_TABLE': user_table.table_name
            }
        )

        create_group_lambda = lambda_.Function(
            self, "create_group_func",
            function_name= "splitwise_create_group_func",
            runtime=lambda_.Runtime.PYTHON_3_8,
            code=lambda_.Code.from_asset('../backend'),
            handler="group_mgr.lambda_handler",
            environment={
                'USER_TABLE': user_table.table_name,
                'GROUP_TABLE': groups_table.table_name,
                'TRANS_TABLE': transactions_table.table_name
            }
        )

        transactions_lambda = lambda_.Function(
            self, "transactions_func",
            function_name= "splitwise_transactions_func",
            runtime=lambda_.Runtime.PYTHON_3_8,
            code=lambda_.Code.from_asset('../backend'),
            handler="transaction_mgr.lambda_handler",
            environment={
                'USER_TABLE': user_table.table_name,
                'GROUP_TABLE': groups_table.table_name,
                'TRANS_TABLE': transactions_table.table_name
            }
        )

        summary_lambda = lambda_.Function(
            self, "summary_func",
            function_name= "splitwise_summary_func",
            runtime=lambda_.Runtime.PYTHON_3_8,
            code=lambda_.Code.from_asset('../backend'),
            handler="summary_mgr.lambda_handler",
            environment={
                'USER_TABLE': user_table.table_name,
                'GROUP_TABLE': groups_table.table_name,
                'TRANS_TABLE': transactions_table.table_name
            }
        )

        # settlements_lambda = lambda_.Function(
        #     self, "settlements_func",
        #     runtime=lambda_.Runtime.PYTHON_3_8,
        #     code=lambda_.Code.from_asset('../backend'),
        #     handler="settlements_mgr.lambda_handler",
        #     environment={
        #         'USER_TABLE': user_table.table_name,
        #         'GROUP_TABLE': groups_table.table_name,
        #         'TRANS_TABLE': transactions_table.table_name
        #     }
        # )

        # Grant permissions
        user_table.grant_read_write_data(create_user_lambda)
        user_table.grant_read_write_data(create_group_lambda)
        user_table.grant_read_data(transactions_lambda)
        user_table.grant_read_data(summary_lambda)
        # user_table.grant_read_data(settlements_lambda)

        transactions_table.grant_read_data(create_group_lambda)
        transactions_table.grant_read_write_data(transactions_lambda)
        transactions_table.grant_read_data(summary_lambda)
        # transactions_table.grant_read_data(settlements_lambda)

        groups_table.grant_read_write_data(create_group_lambda)
        groups_table.grant_read_write_data(transactions_lambda)
        groups_table.grant_read_data(summary_lambda)



        # API gateway
        api = apigateway.RestApi(self, "splitwise_clone")

        # lambda gateway integration
        create_user_integration = apigateway.LambdaIntegration(create_user_lambda)
        create_group_integration = apigateway.LambdaIntegration(create_group_lambda)
        transactions_integration = apigateway.LambdaIntegration(transactions_lambda)
        summary_integration = apigateway.LambdaIntegration(summary_lambda)

        # endpoints and http methods
        users_endpoint = api.root.add_resource('users')
        users_endpoint.add_method('POST', create_user_integration)
        users_endpoint.add_resource('{user_id}').add_method('GET', create_user_integration)

        groups_endpoint = api.root.add_resource('groups')
        groups_endpoint.add_method('POST', create_group_integration)
        groups_endpoint.add_resource('{group_id}').add_method('GET', create_group_integration)

        transactions_endpoint = api.root.add_resource('transactions')
        transactions_endpoint.add_method('POST', transactions_integration)

        summary_endpoint = api.root.add_resource('summary')
        summary_endpoint.add_resource('{group_id}').add_method('GET', summary_integration)

        api.root.add_method("ANY", 
            apigateway.MockIntegration(
                integration_responses=[
                    {
                        "statusCode": "200",
                        "responseTemplates": {
                            "application/json": '{"message": "Splitwise clone. link - https://github.com/rakshithbk/splitwise-backend"}'
                        }
                    }
                ],
                passthrough_behavior=apigateway.PassthroughBehavior.NEVER,
                request_templates={
                    "application/json": '{ "statusCode": 200 }'
                }
            ), 
            method_responses= [
                {
                    "statusCode": "200",
                    "responseParameters": {
                    "method.response.header.Access-Control-Allow-Origin": True,
                    "method.response.header.Access-Control-Allow-Methods": True,
                    "method.response.header.Access-Control-Allow-Headers": True
                    }
                }
            ]
        )

# further upgrades - 
# 1. add cognito api gateway Authentication - https://bobbyhadz.com/blog/aws-cdk-api-authorizer
# 2. 