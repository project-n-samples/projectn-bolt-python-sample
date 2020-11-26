import boto3
import bolt as bolt3
from botocore.exceptions import ClientError
import hashlib


class BoltS3OpsClient:

    def __init__(self):
        self.__s3_client = None

    def process_event(self, event):
        request_type = str(event['requestType']).upper()
        if 'sdkType' in event:
            sdk_type = str(event['sdkType']).upper()
        else:
            sdk_type = 'S3'

        if sdk_type == 'S3':
            self.__s3_client = boto3.client('s3')
        elif sdk_type == 'BOLT':
            self.__s3_client = bolt3.client('s3')

        try:
            if request_type == "LIST_OBJECTS_V2":
                return self.__list_objects_v2(event['bucket'])
            elif request_type == "GET_OBJECT":
                return self.__get_object(event['bucket'], event['key'])
            elif request_type == "HEAD_OBJECT":
                return self.__head_object(event['bucket'], event['key'])
            elif request_type == "LIST_BUCKETS":
                return self.__list_buckets()
            elif request_type == "HEAD_BUCKET":
                return self.__head_bucket(event['bucket'])
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

    def __list_objects_v2(self, bucket):
        resp = self.__s3_client.list_objects_v2(Bucket=bucket)
        objects = [item['Key'] for item in resp['Contents']]
        return {'objects': objects}

    def __get_object(self, bucket, key):
        resp = self.__s3_client.get_object(Bucket=bucket, Key=key)
        md5 = hashlib.md5(resp['Body'].read()).hexdigest().upper()
        return {'md5': md5}

    def __head_object(self, bucket, key):
        resp = self.__s3_client.head_object(Bucket=bucket, Key=key)
        return {
            'Expiration': resp.get('Expiration'),
            'lastModified': resp.get('LastModified').isoformat(),
            'ContentLength': resp.get('ContentLength'),
            'ContentEncoding': resp.get('ContentEncoding'),
            'ETag': resp.get('ETag'),
            'VersionId': resp.get('VersionId'),
            'StorageClass': resp.get('StorageClass')
        }

    def __list_buckets(self):
        resp = self.__s3_client.list_buckets()
        buckets = [bucket['Name'] for bucket in resp['Buckets']]
        return {'buckets': buckets}

    def __head_bucket(self, bucket):
        resp = self.__s3_client.head_bucket(Bucket=bucket)
        headers = resp['ResponseMetadata']['HTTPHeaders']
        status_code = resp['ResponseMetadata']['HTTPStatusCode']
        return {
            'statusCode': status_code,
            'region': headers.get('x-amz-bucket-region')
        }
