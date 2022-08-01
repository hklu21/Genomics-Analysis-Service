# thaw.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import os
import sys
import boto3
import botocore
import json

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import ConfigParser
config = ConfigParser(os.environ)
config.read('thaw_config.ini')

def thaw():
    glacier = boto3.resource("glacier", region_name=config['aws']['AwsRegionName'])
    s3_client = boto3.client('s3', region_name=config['aws']['AwsRegionName'])
    dynamo = boto3.resource('dynamodb', region_name=config['aws']['AwsRegionName'])
    table = dynamo.Table(config['aws']['AwsDynamoTable'])
    sqs = boto3.client('sqs', region_name=config['aws']['AwsRegionName'])
    queue_url = config['aws']['AwsSQSRestoreUrl']
    # Enable long polling on an existing SQS queue
    sqs.set_queue_attributes(
        QueueUrl=queue_url,
        Attributes={'ReceiveMessageWaitTimeSeconds': '20'}
    )
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
            continue
        retrieval_id = message['retrieval_id']
        archive_id = message['archive_id']
        job = glacier.Job('-', config['aws']['VaultName'], retrieval_id)

        print("retrieving {}".format(job.id))
        if job.status_code == 'Succeeded':
            response = job.get_output()
            out_bytes = response['body'].read()
            archive_str = out_bytes.decode('utf-8')
            # restore to file
            file_name = response['archiveDescription'].split('/')[-1]
            job_id = file_name.split('~')[0]
            path = '/'.join(response['archiveDescription'].split('/')[:2])
            with open(file_name, 'a') as out:
                out.write(archive_str)
            
            try:
                # Upload the results file
                s3_client.upload_file(file_name, config['aws']['AwsS3ResultsBuckets'], '{}/{}'.format(path, file_name))
            except boto3.exceptions.S3UploadFailedError:
                print('Error in uploading files!')

            try:
                # change storage status
                table.update_item( Key= {'job_id': str(job_id)},
                                UpdateExpression="SET storage_status = :ss, results_file_archive_id = :a",
                                ExpressionAttributeValues={
                                        ':ss': 'RESTORED',
                                        ':a': ''
                                        },
                                ReturnValues="UPDATED_NEW"
                                )
            except botocore.exceptions.ClientError:
                print('Error in updating table!')
            
            # Clean up (delete) local job files
            try:
                os.system('rm {}'.format(file_name))
            except KeyError:
                print('File not exists!')
            
            # delete archive from glacier
            try:
                archive = glacier.Archive('-', config['aws']['VaultName'], archive_id)
                archive.delete()
            except KeyError:
                print('Error in deleting archive.')

            # Delete the message from the queue, if job was successfully submitted
            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle
            )
            print("File retrieved!")
            

thaw()
### EOF