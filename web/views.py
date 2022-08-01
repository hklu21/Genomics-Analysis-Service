# views.py
#
# Copyright (C) 2011-2020 Vas Vasiliadis
# University of Chicago
#
# Application logic for the GAS
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import uuid
import time
import json
from datetime import datetime
import subprocess


import boto3
from boto3.dynamodb.conditions import Key
from botocore.client import Config
from botocore.exceptions import ClientError

from flask import (abort, flash, redirect, render_template,
  request, session, url_for, jsonify)

from gas import app, db
from decorators import authenticated, is_premium
from auth import get_profile, update_profile


"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document.

Note: You are welcome to use this code instead of your own
but you can replace the code below with your own if you prefer.
"""
@app.route('/annotate', methods=['GET'])
@authenticated
def annotate():
  # Create a session client to the S3 service
  
  s3 = boto3.client('s3',
    region_name=app.config['AWS_REGION_NAME'],
    config=Config(signature_version='s3v4'))

  bucket_name = app.config['AWS_S3_INPUTS_BUCKET']
  user_id = session['primary_identity']

  # Generate unique ID to be used as S3 key (name)
  key_name = app.config['AWS_S3_KEY_PREFIX'] + user_id + '/' + \
    str(uuid.uuid4()) + '~${filename}'

  # Create the redirect URL
  redirect_url = str(request.url) + '/job'

  # Define policy fields/conditions
  encryption = app.config['AWS_S3_ENCRYPTION']
  acl = app.config['AWS_S3_ACL']
  fields = {
    "success_action_redirect": redirect_url,
    "x-amz-server-side-encryption": encryption,
    "acl": acl
  }
  conditions = [
    ["starts-with", "$success_action_redirect", redirect_url],
    {"x-amz-server-side-encryption": encryption},
    {"acl": acl}
  ]


  # Generate the presigned POST call
  try:
    presigned_post = s3.generate_presigned_post(
      Bucket=bucket_name, 
      Key=key_name,
      Fields=fields,
      Conditions=conditions,
      ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
  except ClientError as e:
    app.logger.error(f"Unable to generate presigned URL for upload: {e}")
    return abort(500)
    
  # Render the upload form which will parse/submit the presigned POST
  return render_template('annotate.html', s3_post=presigned_post)


"""Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract 
required info, saves a job item to the database, and then
publishes a notification for the annotator service.

Note: Update/replace the code below with your own from previous
homework assignments
"""
@app.route('/annotate/job', methods=['GET'])
@authenticated
def create_annotation_job_request():

  # Get bucket name, key, and job ID from the S3 redirect URL
  bucket_name = str(request.args.get('bucket'))
  s3_key = str(request.args.get('key'))

  # Extract the job ID from the S3 key
  job_id = s3_key.partition('/')[2].partition('/')[2].partition('~')[0]
  user_id = s3_key.partition('/')[2].partition('/')[0]
  input_file = s3_key.partition('/')[2].partition('/')[2].partition('~')[2]
  

  # if no input file, raise error:
  if input_file == '':
    return render_template('error.html',
      title='Server error', alert_level='danger',
      message="No input file uploaded."
    ), 500

  # Persist job to database
  try:
    # Create a job item and persist it to the annotations database
    dynamo = boto3.resource('dynamodb',
      region_name=app.config['AWS_REGION_NAME'],
      config=Config(signature_version='s3v4'))
    table = dynamo.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
    data = {  "job_id": str(job_id),
              "user_id": str(user_id),
              "input_file_name": str(input_file),
              "s3_inputs_bucket": str(bucket_name),
              "s3_key_input_file": str(s3_key),
              "submit_time": int(time.time()),
              "job_status": "PENDING"
            }

    res = table.put_item(Item=data)
  except botocore.exceptions.ClientError:
    internal_error(error)

  # Send message to request queue
  try:
    # publish a notification message to the SNS topic
    sns = boto3.client('sns',
      region_name=app.config['AWS_REGION_NAME'],
      config=Config(signature_version='s3v4'))
    sns.publish(TopicArn=app.config['AWS_SNS_JOB_REQUEST_TOPIC'], Message=json.dumps(data))
  except botocore.exceptions.ClientError:
    internal_error(error)

  return render_template('annotate_confirm.html', job_id=job_id)


"""List all annotations for the user
"""
@app.route('/annotations', methods=['GET'])
@authenticated
def annotations_list():
  if session.get('role') == 'free_user':
    # archive to glacier vault
    try:
      subprocess.Popen(['sh', '-c', 'cd ../util/archive && python3 archive.py'])
    except subprocess.CalledProcessError:
        internal_error(error)
  elif session.get('role') == 'premium_user':
    #restore()
    try:
      subprocess.Popen(['sh', '-c', 'cd ../util/restore && python3 restore.py'])
    except subprocess.CalledProcessError:
        internal_error(error)

  # Get list of annotations to display
  annotations = []
  s3 = boto3.client("s3", 
    region_name=app.config['AWS_REGION_NAME'],
    config=Config(signature_version='s3v4'))
  all_objects = s3.list_objects(Bucket=app.config['AWS_S3_INPUTS_BUCKET'], Prefix=app.config['AWS_S3_KEY_PREFIX'])

  dynamo = boto3.resource('dynamodb',
      region_name=app.config['AWS_REGION_NAME'],
      config=Config(signature_version='s3v4'))
  table = dynamo.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])

  for i in all_objects['Contents']:
    annotation = {}
    if i['Key'].split('/')[-1].split('~')[-1] != '':
      job_id = i['Key'].split('/')[-1].split('~')[0]
      response = table.get_item(Key ={ 'job_id': job_id})['Item']
      input_file_name = i['Key'].split('/')[-1].split('~')[-1]
      annotation['job_id'] = job_id
      annotation['submit_time'] = datetime.fromtimestamp(response['submit_time'])
      annotation['input_file_name'] = input_file_name
      try:
        annotation['job_status'] = response['job_status']
      except botocore.exceptions.ClientError:
        return render_template('error.html',
          title='Server error', alert_level='danger',
          message="No record in databases."
        ), 500
      annotations.append(annotation)
  
  return render_template('annotations.html', annotations=annotations)


"""Display details of a specific annotation job
"""
@app.route('/annotations/<id>', methods=['GET'])
@authenticated
def annotation_details(id):
  dynamo = boto3.resource('dynamodb',
      region_name=app.config['AWS_REGION_NAME'],
      config=Config(signature_version='s3v4'))
  table = dynamo.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
  response = table.get_item(Key ={ 'job_id': id})['Item']
  annotation = {}
  annotation['job_id'] = id
  annotation['submit_time'] = datetime.fromtimestamp(response['submit_time'])
  annotation['input_file_name'] = response['input_file_name']
  annotation['job_status'] = response['job_status']
  # if not premium, show the driect to subsribe
  free_access_expired = False
  if session.get('role') != 'premium_user':
    free_access_expired = True
  if annotation['job_status'] == 'COMPLETED':
    annotation['complete_time'] = datetime.fromtimestamp(response['complete_time'])
    # get pre-signed url to download
    s3 = boto3.client('s3',
      region_name=app.config['AWS_REGION_NAME'],
      config=Config(signature_version='s3v4'))
    bucket_name = app.config['AWS_S3_RESULTS_BUCKET']
    user_id = session['primary_identity']
    key_name = app.config['AWS_S3_KEY_PREFIX'] + user_id + '/' + \
      id + '~' + str(annotation['input_file_name'])[:-3] + 'annot.vcf'
    try:
      url = s3.generate_presigned_url(
        'get_object',
        Params={'Bucket': bucket_name, 'Key': key_name},
        ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
    except ClientError as e:
      app.logger.error(f"Unable to generate presigned URL for download: {e}")
      return abort(500)
    annotation['result_file_url'] = url
  if 'storage_status' in response and response['storage_status'] == 'RETRIEVING':
    annotation['restore_message'] = "The results file is being restored"

  return render_template('annotation_details.html', annotation=annotation, free_access_expired=free_access_expired)


"""Display the log file contents for an annotation job
"""
@app.route('/annotations/<id>/log', methods=['GET'])
@authenticated
def annotation_log(id):
  dynamo = boto3.resource('dynamodb',
      region_name=app.config['AWS_REGION_NAME'],
      config=Config(signature_version='s3v4'))
  table = dynamo.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
  response = table.get_item(Key ={ 'job_id': id})['Item']
  s3 = boto3.resource('s3',
      region_name=app.config['AWS_REGION_NAME'],
      config=Config(signature_version='s3v4'))
  bucket_name = app.config['AWS_S3_RESULTS_BUCKET']
  user_id = session['primary_identity']
  key_name = app.config['AWS_S3_KEY_PREFIX'] + user_id + '/' + \
    id + '~' + response['input_file_name'] + '.count.log'

  contents = s3.Object(bucket_name, key_name).get()['Body'].read().decode('utf-8')
  return render_template('view_log.html', job_id=id, log_file_contents=contents)


"""Subscription management handler
"""
@app.route('/subscribe', methods=['GET', 'POST'])
@authenticated
def subscribe():
  if (request.method == 'GET'):
    # Display form to get subscriber credit card info
    if (session.get('role') == "free_user"):
      return render_template('subscribe.html')
    else:
      return redirect(url_for('profile'))

  elif (request.method == 'POST'):
    # Update user role to allow access to paid features
    update_profile(
      identity_id=session['primary_identity'],
      role="premium_user"
    )

    # Update role in the session
    session['role'] = "premium_user"

    # Request restoration of the user's data from Glacier
    try:
      subprocess.Popen(['sh', '-c', 'cd ../util/restore && python3 restore.py'])
    except subprocess.CalledProcessError:
        internal_error(error)

    # Display confirmation page
    return render_template('subscribe_confirm.html') 

"""Reset subscription
"""
@app.route('/unsubscribe', methods=['GET'])
@authenticated
def unsubscribe():
  # Hacky way to reset the user's role to a free user; simplifies testing
  update_profile(
    identity_id=session['primary_identity'],
    role="free_user"
  )
  return redirect(url_for('profile'))


"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""

"""Home page
"""
@app.route('/', methods=['GET'])
def home():
  return render_template('home.html')

"""Login page; send user to Globus Auth
"""
@app.route('/login', methods=['GET'])
def login():
  app.logger.info(f"Login attempted from IP {request.remote_addr}")
  # If user requested a specific page, save it session for redirect after auth
  if (request.args.get('next')):
    session['next'] = request.args.get('next')
  return redirect(url_for('authcallback'))

"""404 error handler
"""
@app.errorhandler(404)
def page_not_found(e):
  return render_template('error.html', 
    title='Page not found', alert_level='warning',
    message="The page you tried to reach does not exist. \
      Please check the URL and try again."
    ), 404

"""403 error handler
"""
@app.errorhandler(403)
def forbidden(e):
  return render_template('error.html',
    title='Not authorized', alert_level='danger',
    message="You are not authorized to access this page. \
      If you think you deserve to be granted access, please contact the \
      supreme leader of the mutating genome revolutionary party."
    ), 403

"""405 error handler
"""
@app.errorhandler(405)
def not_allowed(e):
  return render_template('error.html',
    title='Not allowed', alert_level='warning',
    message="You attempted an operation that's not allowed; \
      get your act together, hacker!"
    ), 405

"""500 error handler
"""
@app.errorhandler(500)
def internal_error(error):
  return render_template('error.html',
    title='Server error', alert_level='danger',
    message="The server encountered an error and could \
      not process your request."
    ), 500

### EOF
