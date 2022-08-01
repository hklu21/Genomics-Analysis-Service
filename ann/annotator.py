import uuid
import subprocess
import os
import boto3
import json
import sys

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import ConfigParser
config = ConfigParser(os.environ)
config.read('ann_config.ini')



def annotations():
    # Connect to SQS and get the message queue
    sqs = boto3.client('sqs', region_name=config['aws']['AwsRegionName'])
    queue_url = config['aws']['AwsSQSRequestsUrl']
    # Enable long polling on an existing SQS queue
    sqs.set_queue_attributes(
        QueueUrl=queue_url,
        Attributes={'ReceiveMessageWaitTimeSeconds': '20'}
    )

    # Poll the message queue in a loop
    while True:
        # Attempt to read a message from the queue
        try:
            # Attempt to read a message from the queue
            response = sqs.receive_message(QueueUrl=queue_url)
            message = json.loads(
                json.loads(response['Messages'][0]['Body'])['Message']
            )
            receipt_handle = response['Messages'][0]['ReceiptHandle']
        except KeyError:
            # keep listening until reading an message
            continue

        # If message read, extract job parameters from the message body as before
        key = message['s3_key_input_file']
        name = key.split('/')
        path = '/'.join(name[0:2])
        UUID = message['job_id']
        input_file = message['input_file_name']
        bucket = message['s3_inputs_bucket']
        



        dir_exist = os.path.isdir('jobs')
        if not dir_exist:
            subprocess.Popen(['sh', '-c', 'mkdir jobs'])
        create_ID_folder = 'mkdir jobs/{}'.format(UUID)
        os.system(create_ID_folder)
        
        # Get the input file S3 object and copy it to a local file
        s3_client = boto3.resource('s3', region_name=config['aws']['AwsRegionName'])
        s3_client.meta.client.download_file(bucket, key, '{}'.format(input_file))

    
        move_file = 'mv {} jobs/{}'.format(input_file, UUID)
        os.system(move_file)


        dynamo = boto3.resource('dynamodb', region_name=config['aws']['AwsRegionName'])
        table = dynamo.Table(config['aws']['AwsDynamoTable'])

        response = table.get_item(Key ={ 'job_id': UUID})
        status = response['Item']['job_status']

        # Change your database update code such that the job_status key is 
        # only updated to “RUNNING” only if its current status is “PENDING”.
        if status == 'COMPLETED':
            print({"code": 200,
                            "data": {
                                "job_id": UUID,
                                "input_file": input_file,
                                    },
                            "Comment": "Job has already been completed!"
                })
        elif status == 'PENDING':
            try:
                table.update_item(Key= {'job_id': UUID},
                                UpdateExpression="set job_status = :s",
                                ExpressionAttributeValues={':s': 'RUNNING'},
                                ReturnValues="UPDATED_NEW"
                                )
            except botocore.exceptions.ClientError:
                print({
                    'code': 500,
                    'status': 'error',
                    'message': 'INTERNAL_SERVER_ERROR'
                })

        # Launch annotation job as a background process
        try:
            subprocess.Popen(['sh', '-c', 'cd anntools && python run.py ../jobs/{}/{} {} {} {}'.format(UUID, input_file, UUID, input_file, path)])
        except subprocess.CalledProcessError:
            print({
                    'code': 500,
                    'status': 'error',
                    'message': 'INTERNAL_SERVER_ERROR'
            })
        
        # Delete the message from the queue, if job was successfully submitted
        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )
# run annotations
annotations()