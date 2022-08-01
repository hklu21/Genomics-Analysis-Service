# restore.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import os
import sys
import json
import boto3
import botocore
import subprocess


# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import ConfigParser
config = ConfigParser(os.environ)
config.read('restore_config.ini')

def restore():
    glacier = boto3.resource("glacier", region_name=config['aws']['AwsRegionName'])
    # Connect to SQS and get the message queue
    sqs = boto3.client('sqs', region_name=config['aws']['AwsRegionName'])
    queue_url = config['aws']['AwsSQSArchiveUrl']
    topic_arn = config['aws']['AwsSNSRestoreArn']
    # Enable long polling on an existing SQS queue
    sqs.set_queue_attributes(
        QueueUrl=queue_url,
        Attributes={'ReceiveMessageWaitTimeSeconds': '20'}
    )
    dynamo = boto3.resource('dynamodb', region_name=config['aws']['AwsRegionName'])
    table = dynamo.Table(config['aws']['AwsDynamoTable'])
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
            return
            
        
        archive = glacier.Archive('-', config['aws']['VaultName'], message['archive_id'])
        job = archive.initiate_archive_retrieval()


        try:
            # change storage status
            table.update_item( Key= {'job_id': str(message['job_id'])},
                            UpdateExpression="SET results_file_retrieval_id = :r, storage_status = :ss",
                            ExpressionAttributeValues={
                                    ':r': str(job.id),
                                    ':ss': 'RETRIEVING'
                                    },
                            ReturnValues="UPDATED_NEW"
                            )
        
        except botocore.exceptions.ClientError:
            print('Error in updating table!')
        
        
        # Delete the message from the queue, if job was successfully submitted
        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )

        # Send message to restore queue
        try:
            # publish a notification message to the SNS topic
            data = {"retrieval_id": str(job.id),
                    "archive_id": str(message['archive_id'])
                    }
            sns = boto3.client('sns', region_name=config['aws']['AwsRegionName'])
            sns.publish(TopicArn=topic_arn, Message=json.dumps(data))
        except botocore.exceptions.ClientError:
            print("Error in publishing to SNS topic.")
            

restore()
### EOF