import gzip

import boto3
import bolt as bolt3
from botocore.exceptions import ClientError
import hashlib


class BoltS3OpsClient:
    """
    BoltS3OpsClient processes AWS Lambda events that are received by the handler function
    BoltS3OpsHandler.lambda_handler.
    """

    def __init__(self):
        self._s3_client = None

    def process_event(self, event):
        """
        process_event extracts the parameters (sdkType, requestType, bucket/key) from the event, uses those
        parameters to send an Object/Bucket CRUD request to Bolt/S3 and returns back an appropriate response.
        """

        request_type = str(event['requestType']).upper()

        # request is sent to S3 if 'sdkType' is not passed as a parameter in the event.
        if 'sdkType' in event:
            sdk_type = str(event['sdkType']).upper()
        else:
            sdk_type = 'S3'

        # create an S3/Bolt Client depending on the 'sdkType'
        if sdk_type == 'S3':
            self._s3_client = boto3.client('s3')
        elif sdk_type == 'BOLT':
            self._s3_client = bolt3.client('s3')

        # Performs an S3 / Bolt operation based on the input 'requestType'
        try:
            if request_type == "LIST_OBJECTS_V2":
                return self._list_objects_v2(event['bucket'])
            elif request_type == "GET_OBJECT":
                return self._get_object(event['bucket'], event['key'])
            elif request_type == "HEAD_OBJECT":
                return self._head_object(event['bucket'], event['key'])
            elif request_type == "LIST_BUCKETS":
                return self._list_buckets()
            elif request_type == "HEAD_BUCKET":
                return self._head_bucket(event['bucket'])
            elif request_type == "PUT_OBJECT":
                return self._put_object(event['bucket'], event['key'], event['value'])
            elif request_type == "DELETE_OBJECT":
                return self._delete_object(event['bucket'], event['key'])
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

    def _list_objects_v2(self, bucket):
        """
        Returns a list of 1000 objects from the given bucket in Bolt/S3
        :param bucket: bucket name
        :return: list of first 1000 objects
        """
        resp = self._s3_client.list_objects_v2(Bucket=bucket)
        objects = [item['Key'] for item in resp['Contents']]
        return {'objects': objects}

    def _get_object(self, bucket, key):
        """
        Gets the object from Bolt/S3, computes and returns the object's MD5 hash
        If the object is gzip encoded, object is decompressed before computing its MD5.
        :param bucket: bucket name
        :param key: key name
        :return: md5 hash of the object
        """
        resp = self._s3_client.get_object(Bucket=bucket, Key=key)
        # If Object is gzip encoded, compute MD5 on the decompressed object.
        if ('ContentEncoding' in resp and resp['ContentEncoding'] == 'gzip') or str(key).endswith('.gz'):
            md5 = hashlib.md5(gzip.decompress(resp['Body'].read())).hexdigest().upper()
        else:
            md5 = hashlib.md5(resp['Body'].read()).hexdigest().upper()
        return {'md5': md5}

    def _head_object(self, bucket, key):
        """
        Retrieves the object's metadata from Bolt / S3.
        :param bucket: bucket name
        :param key: key name
        :return: object metadata
        """
        resp = self._s3_client.head_object(Bucket=bucket, Key=key)
        return {
            'Expiration': resp.get('Expiration'),
            'lastModified': resp.get('LastModified').isoformat(),
            'ContentLength': resp.get('ContentLength'),
            'ContentEncoding': resp.get('ContentEncoding'),
            'ETag': resp.get('ETag'),
            'VersionId': resp.get('VersionId'),
            'StorageClass': resp.get('StorageClass')
        }

    def _list_buckets(self):
        """
        Returns list of buckets owned by the sender of the request
        :return: list of buckets
        """
        resp = self._s3_client.list_buckets()
        buckets = [bucket['Name'] for bucket in resp['Buckets']]
        return {'buckets': buckets}

    def _head_bucket(self, bucket):
        """
        Checks if the bucket exists in Bolt/S3.
        :param bucket: bucket name
        :return: status code and Region if the bucket exists
        """
        resp = self._s3_client.head_bucket(Bucket=bucket)
        headers = resp['ResponseMetadata']['HTTPHeaders']
        status_code = resp['ResponseMetadata']['HTTPStatusCode']
        return {
            'statusCode': status_code,
            'region': headers.get('x-amz-bucket-region')
        }

    def _put_object(self, bucket, key, value):
        """
        Uploads an object to Bolt/S3.
        :param bucket: bucket name
        :param key: key name
        :param value: object data
        :return: object metadata
        """
        resp = self._s3_client.put_object(Bucket=bucket, Key=key, Body=str(value).encode())
        return {
            'ETag': resp.get('ETag'),
            'Expiration': resp.get('Expiration'),
            'VersionId': resp.get('VersionId')
        }

    def _delete_object(self, bucket, key):
        """
        Delete an object from Bolt/S3
        :param bucket: bucket name
        :param key: key name
        :return: status code
        """
        resp = self._s3_client.delete_object(Bucket=bucket, Key=key)
        status_code = resp['ResponseMetadata']['HTTPStatusCode']
        return {
            'statusCode': status_code
        }
