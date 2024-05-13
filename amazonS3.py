import boto3

def upload_to_s3(data, bucket, key):
    """
    Uploads data to Amazon S3.

    Args:
        data (bytes): The data to upload.
        bucket (str): The name of the S3 bucket.
        key (str): The key (path) of the object in the bucket.
    """
    s3 = boto3.client('s3')
    s3.put_object(Bucket=bucket, Key=key, Body=data)

data = b'our-raw-data'
bucket_name = 'bargavbucket1'
object_key = 'path/to/your/data.json'
upload_to_s3(data, bucket_name, object_key)
