# archive.py
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
from datetime import datetime
import botocore
import json

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import ConfigParser
config = ConfigParser(os.environ)
config.read('archive_config.ini')

# Add utility code here
def archive():
    glacier = boto3.client("glacier", region_name=config['aws']['AwsRegionName'])
    vault_name = config['aws']['VaultName']
    topic_arn = config['aws']['AwsSNSTopicArn']
    bucket = config['aws']['AwsS3ResultsBuckets']
    dynamo = boto3.resource('dynamodb', region_name=config['aws']['AwsRegionName'])
    table = dynamo.Table(config['aws']['AwsDynamoTable'])
    s3 = boto3.client('s3', region_name=config['aws']['AwsRegionName'])
    s3_client = boto3.resource('s3', region_name=config['aws']['AwsRegionName'])
    all_objects = s3.list_objects(Bucket=bucket, Prefix=config['aws']['AwsS3Prefix'])

    for i in all_objects['Contents']:
        if i['Key'].split('/')[-1].split('~')[-1].split('.')[-1] == 'vcf':
            job_id = i['Key'].split('/')[-1].split('~')[0]
            response = table.get_item(Key ={ 'job_id': job_id})['Item']
            if 'results_file_archive_id' not in response or response['results_file_archive_id'] == '' or response['storage_status'] == 'RESTORED':
                ts = response['complete_time']
                if int(datetime.timestamp(datetime.now())) - ts > 300:
                    # past over 5 mins

                    # archive to glacier vault
                    file_name = i['Key'].split('/')[-1]
                    s3_client.meta.client.download_file(bucket, i['Key'], file_name)

                    try:
                        with open(file_name, 'rb') as upload_file:
                            archive = glacier.upload_archive(vaultName=vault_name,
                                archiveDescription=i['Key'], body=upload_file)
                    except KeyError:
                        print("Vault not exists.")
            
                    try:
                        # capture the object’s Glacier ID
                        table.update_item( Key= {'job_id': str(job_id)},
                                UpdateExpression="SET results_file_archive_id = :a, storage_status = :ss",
                                ExpressionAttributeValues={
                                        ':a': str(archive['ResponseMetadata']['HTTPHeaders']['x-amz-archive-id']),
                                        ':ss': 'ARCHIVED'
                                        },
                                ReturnValues="UPDATED_NEW"
                                )
                    
                    except botocore.exceptions.ClientError:
                        print('Error in updating table!')
                    
                    # remove from s3 bucket
                    try:
                        s3_client.Object(bucket, i['Key']).delete()
                    except botocore.exceptions.ClientError:
                        print('Error in removing file!')

                    # Send message to request queue
                    try:
                        # publish a notification message to the SNS topic
                        data = {  "job_id": str(job_id),
                                "user_id": response['user_id'],
                                "file_name": file_name,
                                "s3_results_bucket": str(bucket),
                                "vault_name": str(vault_name),
                                "archive_id": str(archive['ResponseMetadata']['HTTPHeaders']['x-amz-archive-id'])
                                }
                        sns = boto3.client('sns', region_name=config['aws']['AwsRegionName'])
                        sns.publish(TopicArn=topic_arn, Message=json.dumps(data))
                    except botocore.exceptions.ClientError:
                        print("Error in publishing to SNS topic.")

                    # Clean up (delete) local job files
                    try:
                        os.system('rm {}'.format(file_name))
                    except KeyError:
                        print('File not exists!')
                    
                    print("File archived. Job id: {}".format(job_id))
      
archive()
### EOF