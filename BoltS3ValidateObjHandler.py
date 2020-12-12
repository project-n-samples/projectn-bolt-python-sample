import boto3
import bolt as bolt3
from botocore.exceptions import ClientError
import hashlib
import gzip


def lambda_handler(event, context):
    """
    lambda_handler is the handler function that is invoked by AWS Lambda to process an incoming event for
    performing data validation tests.

    lambda_handler accepts the following input parameters as part of the event:
    1) bucket - bucket name
    2) key - key name

    lambda_handler retrieves the object from Bolt and S3 (if BucketClean is OFF), computes and returns their
    corresponding MD5 hash. If the object is gzip encoded, object is decompressed before computing its MD5.

    :param event: incoming event data
    :param context: runtime information
    :return: md5s of object retrieved from Bolt and S3
    """
    bucket = event['bucket']
    key = event['key']
    if 'bucketClean' in event:
        bucket_clean = str(event['bucketClean']).upper()
    else:
        bucket_clean = 'OFF'

    s3_client = boto3.client('s3')
    bolts3_client = bolt3.client('s3')

    try:
        # Get Object from Bolt
        bolt_resp = bolts3_client.get_object(Bucket=bucket, Key=key)
        # Get Object from S3 if bucket clean is off
        if bucket_clean == 'OFF':
            s3_resp = s3_client.get_object(Bucket=bucket, Key=key)

        # Parse the MD5 of the returned object.
        # If Object is gzip encoded, compute MD5 on the decompressed object.
        if ('ContentEncoding' in s3_resp and s3_resp['ContentEncoding'] == 'gzip') or str(key).endswith('.gz'):
            bolt_md5 = hashlib.md5(gzip.decompress(bolt_resp['Body'].read())).hexdigest().upper()
            s3_md5 = hashlib.md5(gzip.decompress(s3_resp['Body'].read())).hexdigest().upper()
        else:
            bolt_md5 = hashlib.md5(bolt_resp['Body'].read()).hexdigest().upper()
            s3_md5 = hashlib.md5(s3_resp['Body'].read()).hexdigest().upper()
        return {
            's3-md5': s3_md5,
            'bolt-md5': bolt_md5
        }
    except ClientError as e:
        return {
            'errorMessage': e.response['Error']['Message'],
            'errorCode': e.response['Error']['Code']
        }
    except Exception as e:
        return {
            'errorMessage': str(e),
            'errorCode': str(1)
        }
