import boto.sqs

def getQueue(queue_name):
  print "Enter getQueue"
  print queue_name
  conn = boto.sqs.connect_to_region('us-west-2')
  my_queue = conn.lookup(queue_name)
  my_queue.get_attributes()

getQueue('bdt_daily')
