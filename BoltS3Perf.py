import boto3
import bolt as bolt3
from botocore.exceptions import ClientError
import random
import string
import time
from statistics import mean


class BoltS3Perf:

    def __init__(self):
        # create S3 and Bolt Clients
        self._s3_client = boto3.client('s3')
        self._bolts3_client = bolt3.client('s3')

    def process_event(self, event):

        # if requestType is not passed, perform all perf tests.
        if 'requestType' in event:
            request_type = str(event['requestType']).upper()
        else:
            request_type = 'ALL'

        # Perform Perf tests based on input 'requestType'
        try:
            if request_type == "PUT_OBJECT":
                return self._put_object_perf(event['bucket'], event['keys'])
            elif request_type == "GET_OBJECT":
                return self._get_object_perf(event['bucket'], event['keys'])
            elif request_type == "DELETE_OBJECT":
                return self._delete_object_perf(event['bucket'], event['keys'])
            elif request_type == "LIST_OBJECTS_V2":
                return self._list_objects_v2_perf(event['bucket'])
            elif request_type == "ALL":
                return self._all_perf(event['bucket'], event['keys'])
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

    def _put_object_perf(self, bucket, keys):
        s3_put_obj_times = []
        bolt_put_obj_times = []

        for key in keys:
            value = self._generate(characters=string.ascii_lowercase, length=100 * 1000)
            value_bytes = value.encode()

            put_obj_start_time = time.time()
            self._s3_client.put_object(Bucket=bucket, Key=key, Body=value_bytes)
            put_obj_end_time = time.time()
            s3_put_obj_times.append(put_obj_end_time - put_obj_start_time)

            put_obj_start_time = time.time()
            self._bolts3_client.put_object(Bucket=bucket, Key=key, Body=value_bytes)
            put_obj_end_time = time.time()
            bolt_put_obj_times.append(put_obj_end_time - put_obj_start_time)

        s3_put_obj_avg_time = mean(s3_put_obj_times)
        bolt_put_obj_avg_time = mean(bolt_put_obj_times)

        return {
            's3_put_obj_time': "{:.2f} secs".format(s3_put_obj_avg_time),
            'bolt_put_obj_time': "{:.2f} secs".format(bolt_put_obj_avg_time)
        }

    def _generate(self, characters=string.ascii_lowercase, length=10):
        return ''.join(random.choice(characters) for _ in range(length))

    def _get_object_perf(self, bucket, keys):

        s3_get_obj_times = []
        bolt_get_obj_times = []

        for key in keys:
            get_obj_start_time = time.time()
            self._s3_client.get_object(Bucket=bucket, Key=key)
            get_obj_end_time = time.time()
            s3_get_obj_times.append(get_obj_end_time - get_obj_start_time)

            get_obj_start_time = time.time()
            self._bolts3_client.get_object(Bucket=bucket, Key=key)
            get_obj_end_time = time.time()
            bolt_get_obj_times.append(get_obj_end_time - get_obj_start_time)

        s3_get_obj_avg_time = mean(s3_get_obj_times)
        bolt_get_obj_avg_time = mean(bolt_get_obj_times)

        return {
            's3_get_obj_time': "{:.2f} secs".format(s3_get_obj_avg_time),
            'bolt_get_obj_time': "{:.2f} secs".format(bolt_get_obj_avg_time)
        }

    def _delete_object_perf(self, bucket, keys):

        s3_del_obj_times = []
        bolt_del_obj_times = []

        for key in keys:
            del_obj_start_time = time.time()
            self._s3_client.delete_object(Bucket=bucket, Key=key)
            del_obj_end_time = time.time()
            s3_del_obj_times.append(del_obj_end_time - del_obj_start_time)

            del_obj_start_time = time.time()
            self._bolts3_client.delete_object(Bucket=bucket, Key=key)
            del_obj_end_time = time.time()
            bolt_del_obj_times.append(del_obj_end_time - del_obj_start_time)

        s3_del_obj_avg_time = mean(s3_del_obj_times)
        bolt_del_obj_avg_time = mean(bolt_del_obj_times)

        return {
            's3_del_obj_time': "{:.2f} secs".format(s3_del_obj_avg_time),
            'bolt_del_obj_time': "{:.2f} secs".format(bolt_del_obj_avg_time)
        }

    def _list_objects_v2_perf(self, bucket):

        s3_list_objects_v2_times = []
        bolt_list_objects_v2_times = []

        for x in range(3):
            list_objects_v2_start_time = time.time()
            s3_paginator = self._s3_client.get_paginator('list_objects_v2')
            page_iterator = s3_paginator.paginate(Bucket=bucket)
            for page in page_iterator:
                pass
            list_objects_v2_end_time = time.time()
            s3_list_objects_v2_times.append(list_objects_v2_end_time - list_objects_v2_start_time)

            list_objects_v2_start_time = time.time()
            bolt_paginator = self._bolts3_client.get_paginator('list_objects_v2')
            page_iterator = bolt_paginator.paginate(Bucket=bucket)
            for page in page_iterator:
                pass
            list_objects_v2_end_time = time.time()
            bolt_list_objects_v2_times.append(list_objects_v2_end_time - list_objects_v2_start_time)

        s3_list_objects_v2_avg_time = mean(s3_list_objects_v2_times)
        bolt_list_objects_v2_avg_time = mean(bolt_list_objects_v2_times)

        return {
            's3_list_objects_v2_time': "{:.2f} secs".format(s3_list_objects_v2_avg_time),
            'bolt_list_objects_v2_time': "{:.2f} secs".format(bolt_list_objects_v2_avg_time)
        }

    def _all_perf(self, bucket, keys):
        put_obj_perf_stats = self._put_object_perf(bucket, keys)
        get_obj_perf_stats = self._get_object_perf(bucket, keys)
        del_obj_perf_stats = self._delete_object_perf(bucket, keys)
        list_objects_v2_perf_stats = self._list_objects_v2_perf(bucket)

        return self._merge_perf_stats(put_obj_perf_stats,
                                      get_obj_perf_stats,
                                      del_obj_perf_stats,
                                      list_objects_v2_perf_stats)

    def _merge_perf_stats(self, *perf_stats):
        merged_perf_stats = {}
        for perf_stat in perf_stats:
            merged_perf_stats.update(perf_stat)
        return merged_perf_stats
