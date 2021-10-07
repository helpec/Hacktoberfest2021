import os
import json
import time

from google.cloud import pubsub_v1

subscriber = pubsub_v1.SubscriberClient()
subscription_path = "projects/bigdata/subscriptions/items-delivery-time"


outfile = open('./delivery-time.jsonl', 'bw')

def callback(message):
    outfile.write(message.data)
    outfile.write(b'\n')
    message.ack()
    
# The subscriber is non-blocking. We must keep the main thread from
# exiting to allow it to process messages asynchronously in the background.
# print("Listening for messages on {}".format(subscription_path))
while True:
    subscriber.subscribe(subscription_path, callback=callback)
    time.sleep(5)
