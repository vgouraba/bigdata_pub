import boto3

def listQueues(sqs):
  "Function to list all queueus"
  response = sqs.list_queues()
  return response

def putMessage(sqs, queue_url):
  response = sqs.send_message(
    QueueUrl=queue_url,
    #MessageGroupId='grp_1',
    MessageBody=(
        'Start the Process.'
    )
  )
  print(response['MessageId'])

sqs = boto3.client('sqs')

queue = listQueues(sqs)
print(queue['QueueUrls'])

putMessage(sqs, 'https://us-west-2.queue.amazonaws.com/xxx/bdt_daily')
