# run.py
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
#
# Wrapper script for running AnnTools
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import sys
import time
import driver
import boto3
import os
import botocore
import json

# Import utility helpers
sys.path.insert(0, '../../util')
import helpers

# Get configuration
from configparser import ConfigParser
config = ConfigParser(os.environ)
config.read('../ann_config.ini')


"""A rudimentary timer for coarse-grained profiling
"""


class Timer(object):
    def __init__(self, verbose=True):
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        if self.verbose:
            print(f"Approximate runtime: {self.secs:.2f} seconds")


if __name__ == '__main__':
    # Call the AnnTools pipeline
    if len(sys.argv) > 1:
        with Timer():
            driver.run(sys.argv[1], 'vcf')
        s3_client = boto3.client('s3', region_name=config['aws']['AwsRegionName'])
        UUID = sys.argv[2]
        input_file = sys.argv[3]
        path = sys.argv[4]
        prefix = input_file.partition('.')[0]

        try:
            # Upload the results file
            s3_client.upload_file('../jobs/{}/{}.annot.vcf'.format(UUID, prefix), config['aws']['AwsS3ResultsBuckets'], '{}/{}~{}.annot.vcf'.format(path, UUID, prefix))
            # Upload the log file
            s3_client.upload_file('../jobs/{}/{}.vcf.count.log'.format(UUID, prefix), config['aws']['AwsS3ResultsBuckets'], '{}/{}~{}.vcf.count.log'.format(path, UUID, prefix))
        except boto3.exceptions.S3UploadFailedError:
            print('Error in uploading files!')

        
        try:
            # Updates the job item in your DynamoDB table
            dynamo = boto3.resource('dynamodb', region_name=config['aws']['AwsRegionName'])
            table = dynamo.Table(config['aws']['AwsDynamoTable'])
            table.update_item( Key= {'job_id': UUID},
                            UpdateExpression="set job_status = :s, s3_results_bucket = :rb, s3_key_result_file = :rf, s3_key_log_file = :lf, complete_time = :ct",
                            ExpressionAttributeValues={
                                    ':s': 'COMPLETED',
                                    ':rb': config['aws']['AwsS3ResultsBuckets'],
                                    ':rf': '{}/{}~{}.annot.vcf'.format(path, UUID, prefix),
                                    ':lf': '{}/{}~{}.vcf.count.log'.format(path, UUID, prefix),
                                    ':ct': int(time.time())
                                    },
                            ReturnValues="UPDATED_NEW"
                            )
        except botocore.exceptions.ClientError:
            print('Error in updating table!')
        
        # Send message to request queue
        profile = helpers.get_user_profile(id=table.get_item(Key ={ 'job_id': UUID})['Item']['user_id'])
        try:
            # publish a notification message to the SNS topic
            sns = boto3.client('sns', region_name=config['aws']['AwsRegionName'])
            response = table.get_item(Key ={ 'job_id': UUID})['Item']
            data = {  "job_id": response['job_id'],
              "user_id": response['user_id'],
              "input_file_name": response['input_file_name'],
              "s3_inputs_bucket": response['s3_inputs_bucket'],
              "s3_key_input_file": response['s3_key_input_file'],
              "submit_time": int(time.time()),
              "job_status": response['job_status'],
              "user_email": profile[2]
            }
            sns.publish(TopicArn=config['aws']['AwsSNSResultsArn'], Message=json.dumps(data))
        except botocore.exceptions.ClientError:
            print('Error in publishing a notification!')
        
        # Clean up (delete) local job files
        try:
            os.system('rm -r ../jobs/{}'.format(UUID))
        except KeyError:
            print('File not exists!')

    else:
        print("A valid .vcf file must be provided as input to this program.")
### EOF
