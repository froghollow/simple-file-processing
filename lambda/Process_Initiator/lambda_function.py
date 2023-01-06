import json
import datetime
import os
import boto3

#import batch_functions as bat  # ToDo: refactor from v4

def lambda_handler(event, context):
    ''' Triggered by EventBridge to Execute a Step Function ARN Specified in the Event's Input Transformer '''
    print("Received event: " + json.dumps(event))
    response = event

    if 'source' in event.keys():
        # triggered by EventBridge ...
        if event['source'] == 'aws.s3':
            s3_bucket = event['detail']['bucket']['name']
            s3_key = event['detail']['object']['key']
        else:
            Exception(f"Unknown Event Source '{event['source']}'.")
    else:
        raise Exception("event['source'] not specified.")

    batch_id = s3_key.split('/')[-1]
    response['batch_parms']['BatchId'] = batch_id

    ''' ToDo:  Refactor from v4 >>>
    # create or fetch Batch
    batch = bat.Batch( batch_id ) 

    if len(batch.batch['BatchObjects']) > 0:    
        response['batch_parms']['NameList'] = batch.get_batch_objects()

    if len(batch.batch['BatchObjects']) == 0 and 'NameList' in event['batch_parms'].keys():
        batch.initialize_batch_objects( event['batch_parms']['NameList'] )

    <<< '''

    if 'StepFnArn' in event['batch_parms'].keys():
        step_arn = event['batch_parms']['StepFnArn']
    elif 'StepFnArn' in os.environ.keys():
        step_arn = os.environ['StepFnArn']
        response['batch_parms']['StepFnArn'] = step_arn
    else:
        #raise Exception("Can't find a Step Function ARN to execute!")
        print("Can't find a Step Function ARN to execute!")
        step_arn = ''
    
    if step_arn > '':
        # execute Step Function if ARN specified ...
        step_arn = step_arn.replace('$Stack', context.function_name[:context.function_name.rfind('-')])

        now = datetime.datetime.now().strftime("%y%m%d-%H%M%S")
        exec_name = f"{now}-{batch_id}"
        response['batch_parms']['ExecName'] = exec_name

        # Execute State Machine (here in Lambda, not EventBridge, to support batch_parms exec_name)
        sfn_client = boto3.client('stepfunctions')
        sfn_resp = sfn_client.start_execution(
            stateMachineArn = step_arn,
            name = exec_name,
            input= json.dumps(response)
        )
        response.update( {
            "StepFnExecArn" : sfn_resp['executionArn']
        } )

    print('Response: ' + json.dumps(response))
    return {
        'statusCode': 200,
        'body': response
    }
