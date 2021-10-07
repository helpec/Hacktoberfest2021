"""Publishes multiple messages to a Pub/Sub topic with an error handler."""
import os

from concurrent import futures
from google.cloud import pubsub_v1

project_id = "bigdata"
topic_id = "catalog-items-delivery-time-stg"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)
publish_futures = []

def get_callback(publish_future, data):
    def callback(publish_future):
        try:
            # Wait 60 seconds for the publish call to succeed.
            print(publish_future.result(timeout=60))
        except futures.TimeoutError:
            print(f"Publishing {data} timed out.")

    return callback

with open('./delivery-time.jsonl', 'r') as outfile:
    for data in outfile.readlines():
        
        # When you publish a message, the client returns a future.
        publish_future = publisher.publish(topic_path, data.encode("utf-8"))
        
        # Non-blocking. Publish failures are handled in the callback function.
        publish_future.add_done_callback(get_callback(publish_future, data))
        publish_futures.append(publish_future)

# Wait for all the publish futures to resolve before exiting.
# futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)

print(f"Published messages with error handler to {topic_path}.")
