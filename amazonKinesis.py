import boto3
import json
import random
import time

kinesis = boto3.client('kinesis', region_name='us-east-1')

def put_to_kinesis(data):
    kinesis.put_record(
        StreamName='TrafficDataStream2',
        Data=json.dumps(data),
        PartitionKey='<event>'
    )

while True:
    # Simulating traffic data
    timestamp = time.strftime('%Y-%m-%dT%H:%M:%S')
    location = f"lat: {random.uniform(40, 41)}, lon: {random.uniform(-74, -73)}"
    traffic_status = random.choice(["light", "moderate", "heavy"])
    # Sending data to Kinesis
    sample_data = {"timestamp": timestamp, "location": location, "traffic_status": traffic_status}
    put_to_kinesis(sample_data)
    time.sleep(1)