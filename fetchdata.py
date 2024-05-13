import requests
import json
import boto3

def fetch_data_from_api():
    api_url = 'http://api.511.org/traffic/events?api_key=70835821-b8f4-48e8-8d55-a1dafa7da748'
    headers = {
        'Authorization': 'Bearer 70835821-b8f4-48e8-8d55-a1dafa7da748'
    }

    # Initialize boto3 client for Kinesis
    kinesis = boto3.client('kinesis', region_name='us-east-1')
    stream_name = 'projecttrafficdata' 

    response = requests.get(api_url, headers=headers)
    if response.status_code == 200:
        print("Data fetched successfully!")
        try:
            # Decode JSON data and send to Kinesis
            json_data = response.content.decode('utf-8-sig')
            data = json.loads(json_data)
            print(data)

            # Send the data to Kinesis stream
            response_kinesis = kinesis.put_record(
                StreamName=stream_name,
                Data=json.dumps(data),  
                PartitionKey='partition_key'
            )
            print("Data sent to Kinesis successfully:", response_kinesis)
        except Exception as e:
            print("Failed to decode JSON data or send to Kinesis:", str(e))
    else:
        print("Failed to retrieve data. Status code:", response.status_code)
        print(response.text)

fetch_data_from_api()

