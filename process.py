import boto3

def listQueues(sqs):
  "Function to list all queueus"
  response = sqs.list_queues()
  return response

def getQueue(sqs,queue_url):
  print "getQueue Attrs"
  print queue_url
  response = sqs.get_queue_attributes(
    QueueUrl=queue_url,
    AttributeNames=[
        'All'
    ]
  )
  response

def getMessage(sqs, queue_url):
  response = sqs.receive_message(
    QueueUrl=queue_url,
    AttributeNames=[
        'SentTimestamp'
    ],
    MaxNumberOfMessages=1,
    MessageAttributeNames=[
        'All'
    ],
    VisibilityTimeout=0,
    WaitTimeSeconds=10
  )

  message = response['Messages'][0]
  receipt_handle = message['ReceiptHandle']
  
  # Delete received message from queue
  sqs.delete_message(
    QueueUrl=queue_url,
    ReceiptHandle=receipt_handle
  )
  print('Received and deleted message: %s' % message)


sqs = boto3.client('sqs')

queue = listQueues(sqs)
print(queue['QueueUrls'])

#getQueue(sqs, 'https://us-west-2.queue.amazonaws.com/097884913175/bdt_daily')
getMessage(sqs, 'https://us-west-2.queue.amazonaws.com/097884913175/bdt_daily')

# If we come here mean...we can process Spark
print "Process Spark"

import os
os.system('spark-submit --packages com.databricks:spark-csv_2.10:1.5.0 spark/dailyFunc.py')

