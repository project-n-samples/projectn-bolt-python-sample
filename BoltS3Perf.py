import boto3
import bolt as bolt3
from botocore.exceptions import ClientError
import random
import string
import time
import math
from statistics import mean
from statistics import median_low


class BoltS3Perf:
    """
    BoltS3Perf processes AWS Lambda events that are received by the handler function
    BoltS3OpsHandler.perf_lambda_handler for Bolt/S3 Performance testing.
    """

    # constants for PUT/DELETE Object Perf
    # max. no of keys to be used in
    NUM_KEYS = 1000
    # length of object data
    OBJ_LENGTH = 100

    def __init__(self):
        # create S3 and Bolt Clients
        self._s3_client = boto3.client('s3')
        self._bolts3_client = bolt3.client('s3')
        self._keys = None
        self._request_type = None

    def process_event(self, event):
        """
        process_event extracts the parameters (requestType, bucket) from the event, uses those
        parameters to run performance testing against Bolt/S3 and returns back performance statistics.
        :param event: incoming event data
        :return: performance statistics
        """

        # if requestType is not passed, perform all perf tests.
        if 'requestType' in event:
            self._request_type = str(event['requestType']).upper()
        else:
            self._request_type = 'ALL'

        # update max. no of keys and object data length, if passed in input.
        if 'numKeys' in event:
            self.NUM_KEYS = int(event['numKeys'])
            if self.NUM_KEYS > 1000:
                self.NUM_KEYS = 1000
        if 'objLength' in event:
            self.OBJ_LENGTH = int(event['objLength'])

        # if keys not passed as in input:
        # if GET_OBJECT or GET_OBJECT_PASSTHROUGH, list objects (up to NUM_KEYS) to get key names
        # otherwise generate key names.
        if 'keys' in event:
            self._keys = event['keys']
        elif self._request_type == "GET_OBJECT" or self._request_type == "GET_OBJECT_PASSTHROUGH" or\
                self._request_type == "GET_OBJECT_TTFB" or self._request_type == "GET_OBJECT_PASSTHROUGH_TTFB":
            self._keys = self._list_objects_v2(event['bucket'])
        else:
            self._keys = self._generate_key_names(self.NUM_KEYS)

        # Perform Perf tests based on input 'requestType'
        try:
            if self._request_type == "PUT_OBJECT":
                return self._put_object_perf(event['bucket'])
            elif self._request_type == "GET_OBJECT" or self._request_type == "GET_OBJECT_TTFB":
                return self._get_object_perf(event['bucket'])
            elif self._request_type == "GET_OBJECT_PASSTHROUGH" or self._request_type == "GET_OBJECT_PASSTHROUGH_TTFB":
                return self._get_object_passthrough_perf(event['bucket'])
            elif self._request_type == "DELETE_OBJECT":
                return self._delete_object_perf(event['bucket'])
            elif self._request_type == "LIST_OBJECTS_V2":
                return self._list_objects_v2_perf(event['bucket'])
            elif self._request_type == "ALL":
                return self._all_perf(event['bucket'])
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

    def _put_object_perf(self, bucket):
        """
        Measures the Put Object performance (latency, throughput) of Bolt / S3.
        :param bucket: bucket name
        :return: Put object performance statistics
        """
        s3_put_obj_times = []
        bolt_put_obj_times = []

        # Upload objects to Bolt / S3.
        for key in self._keys:
            value = self._generate(characters=string.ascii_lowercase, length=self.OBJ_LENGTH)
            value_bytes = value.encode()

            # Upload object to S3.
            put_obj_start_time = time.time()
            self._s3_client.put_object(Bucket=bucket, Key=key, Body=value_bytes)
            put_obj_end_time = time.time()
            # calc latency
            put_obj_time = put_obj_end_time - put_obj_start_time
            s3_put_obj_times.append(put_obj_time)

            # Upload object to Bolt.
            put_obj_start_time = time.time()
            self._bolts3_client.put_object(Bucket=bucket, Key=key, Body=value_bytes)
            put_obj_end_time = time.time()
            # calc latency
            put_obj_time = put_obj_end_time - put_obj_start_time
            bolt_put_obj_times.append(put_obj_time)

        # calc s3 perf stats
        s3_put_obj_perf_stats = self._compute_perf_stats(s3_put_obj_times)

        # calc bolt perf stats
        bolt_put_obj_perf_stats = self._compute_perf_stats(bolt_put_obj_times)

        return {
            'object_size': "{:d} bytes".format(self.OBJ_LENGTH),
            's3_put_obj_perf_stats': s3_put_obj_perf_stats,
            'bolt_put_obj_perf_stats': bolt_put_obj_perf_stats
        }

    def _get_object_perf(self, bucket):
        """
        Measures the Get Object performance (latency, throughput) of Bolt / S3.
        :param bucket: bucket name
        :return: Get Object performance statistics
        """

        # list of latencies.
        s3_get_obj_times = []
        bolt_get_obj_times = []

        # list of object sizes.
        s3_obj_sizes = []
        bolt_obj_sizes = []

        # object counts (compressed, uncompressed).
        s3_cmp_obj_count = 0
        s3_uncmp_obj_count = 0
        bolt_cmp_obj_count = 0
        bolt_uncmp_obj_count = 0

        # Get Objects from S3.
        for key in self._keys:
            get_obj_start_time = time.time()
            s3_resp = self._s3_client.get_object(Bucket=bucket, Key=key)
            # If getting first byte object latency, read at most 1 byte
            # otherwise read the entire body.
            if self._request_type == "GET_OBJECT_TTFB":
                # read only first byte from StreamingBody.
                s3_resp['Body'].read(amt=1)
            else:
                # read all the data from StreamingBody.
                for chunk in s3_resp['Body'].iter_chunks():
                    pass
            get_obj_end_time = time.time()
            # calc latency
            get_obj_time = get_obj_end_time - get_obj_start_time
            s3_get_obj_times.append(get_obj_time)
            # count object
            if ('ContentEncoding' in s3_resp and s3_resp['ContentEncoding'] == 'gzip') or str(key).endswith('.gz'):
                s3_cmp_obj_count += 1
            else:
                s3_uncmp_obj_count += 1
            # get object size.
            if 'ContentLength' in s3_resp:
                s3_obj_sizes.append(s3_resp['ContentLength'])

        # Get Objects from Bolt.
        for key in self._keys:
            get_obj_start_time = time.time()
            bolt_resp = self._bolts3_client.get_object(Bucket=bucket, Key=key)
            # If getting first byte object latency, read at most 1 byte
            # otherwise read the entire body.
            if self._request_type == "GET_OBJECT_TTFB":
                # read only first byte from StreamingBody.
                bolt_resp['Body'].read(amt=1)
            else:
                # read all the data from StreamingBody.
                for chunk in bolt_resp['Body'].iter_chunks():
                    pass
            get_obj_end_time = time.time()
            # calc latency
            get_obj_time = get_obj_end_time - get_obj_start_time
            bolt_get_obj_times.append(get_obj_time)
            # count object
            if ('ContentEncoding' in bolt_resp and bolt_resp['ContentEncoding'] == 'gzip') or str(key).endswith('.gz'):
                bolt_cmp_obj_count += 1
            else:
                bolt_uncmp_obj_count += 1
            # get object size.
            if 'ContentLength' in bolt_resp:
                bolt_obj_sizes.append(bolt_resp['ContentLength'])

        # calc s3 perf stats
        s3_get_obj_perf_stats = self._compute_perf_stats(s3_get_obj_times, obj_sizes=s3_obj_sizes)

        # calc bolt perf stats
        bolt_get_obj_perf_stats = self._compute_perf_stats(bolt_get_obj_times, obj_sizes=bolt_obj_sizes)

        # assign perf stats name.
        if self._request_type == "GET_OBJECT_TTFB":
            s3_get_obj_stat_name = 's3_get_obj_ttfb_perf_stats'
            bolt_get_obj_stat_name = 'bolt_get_obj_ttfb_perf_stats'
        else:
            s3_get_obj_stat_name = 's3_get_obj_perf_stats'
            bolt_get_obj_stat_name = 'bolt_get_obj_perf_stats'

        return {
            s3_get_obj_stat_name: s3_get_obj_perf_stats,
            's3_object_count (compressed)': s3_cmp_obj_count,
            's3_object_count (uncompressed)': s3_uncmp_obj_count,
            bolt_get_obj_stat_name: bolt_get_obj_perf_stats,
            'bolt_object_count (compressed)': bolt_cmp_obj_count,
            'bolt_object_count (uncompressed)': bolt_uncmp_obj_count
        }

    def _get_object_passthrough_perf(self, bucket):
        """
        Measures the Get Object passthrough performance (latency, throughput) of Bolt / S3.
        :param bucket: name of unmonitored bucket
        :return: Get Object passthrough performance statistics
        """

        # list of latencies
        bolt_get_obj_times = []

        # list of object sizes
        bolt_obj_sizes = []

        # object counts (compressed, uncompressed).
        bolt_cmp_obj_count = 0
        bolt_uncmp_obj_count = 0

        # Get Objects via passthrough from Bolt.
        for key in self._keys:
            get_obj_start_time = time.time()
            bolt_resp = self._bolts3_client.get_object(Bucket=bucket, Key=key)
            # If getting first byte object latency, read at most 1 byte
            # otherwise read the entire body.
            if self._request_type == "GET_OBJECT_PASSTHROUGH_TTFB":
                # read only first byte from StreamingBody.
                bolt_resp['Body'].read(amt=1)
            else:
                # read all the data from StreamingBody.
                for chunk in bolt_resp['Body'].iter_chunks():
                    pass
            get_obj_end_time = time.time()
            # calc latency
            get_obj_time = get_obj_end_time - get_obj_start_time
            bolt_get_obj_times.append(get_obj_time)
            # count object
            if ('ContentEncoding' in bolt_resp and bolt_resp['ContentEncoding'] == 'gzip') or str(key).endswith('.gz'):
                bolt_cmp_obj_count += 1
            else:
                bolt_uncmp_obj_count += 1
            # get object size.
            if 'ContentLength' in bolt_resp:
                bolt_obj_sizes.append(bolt_resp['ContentLength'])

        # calc bolt perf stats
        bolt_get_obj_pt_perf_stats = self._compute_perf_stats(bolt_get_obj_times, obj_sizes=bolt_obj_sizes)

        # assign perf stats name.
        if self._request_type == "GET_OBJECT_PASSTHROUGH_TTFB":
            bolt_get_obj_pt_stat_name = 'bolt_get_obj_pt_ttfb_perf_stats'
        else:
            bolt_get_obj_pt_stat_name = 'bolt_get_obj_pt_perf_stats'

        return {
            bolt_get_obj_pt_stat_name: bolt_get_obj_pt_perf_stats,
            'bolt_object_count (compressed)': bolt_cmp_obj_count,
            'bolt_object_count (uncompressed)': bolt_uncmp_obj_count
        }

    def _delete_object_perf(self, bucket):
        """
        Measures the Delete Object performance (latency, throughput) of Bolt/S3.
        :param bucket: bucket name
        :return: Delete Object performance statistics
        """

        s3_del_obj_times = []
        bolt_del_obj_times = []

        # Delete Objects from S3.
        for key in self._keys:
            del_obj_start_time = time.time()
            self._s3_client.delete_object(Bucket=bucket, Key=key)
            del_obj_end_time = time.time()
            # calc latency
            del_obj_time = del_obj_end_time - del_obj_start_time
            s3_del_obj_times.append(del_obj_time)

        # Delete Objects from Bolt.
        for key in self._keys:
            del_obj_start_time = time.time()
            self._bolts3_client.delete_object(Bucket=bucket, Key=key)
            del_obj_end_time = time.time()
            # calc latency.
            del_obj_time = del_obj_end_time - del_obj_start_time
            bolt_del_obj_times.append(del_obj_time)

        # calc s3 perf stats
        s3_del_obj_perf_stats = self._compute_perf_stats(s3_del_obj_times)

        # calc bolt perf stats
        bolt_del_obj_perf_stats = self._compute_perf_stats(bolt_del_obj_times)

        return {
            's3_del_obj_perf_stats': s3_del_obj_perf_stats,
            'bolt_del_obj_perf_stats': bolt_del_obj_perf_stats
        }

    def _list_objects_v2_perf(self, bucket, num_iter=10):
        """
        Measures the List Objects V2 performance (latency, throughput) of Bolt / S3.
        :param bucket: bucket name
        :param num_iter: num of iterations
        :return: List Objects V2 performance statistics
        """

        s3_list_objects_v2_times = []
        bolt_list_objects_v2_times = []
        s3_list_objects_v2_tp = []
        bolt_list_objects_v2_tp = []

        # list 1000 objects from S3, num_iter times.
        for x in range(num_iter):
            list_objects_v2_start_time = time.time()
            s3_resp = self._s3_client.list_objects_v2(Bucket=bucket)
            list_objects_v2_end_time = time.time()
            # calc latency
            list_objects_v2_time = list_objects_v2_end_time - list_objects_v2_start_time
            s3_list_objects_v2_times.append(list_objects_v2_time)
            # calc throughput
            list_objects_tp = s3_resp['KeyCount'] / list_objects_v2_time
            s3_list_objects_v2_tp.append(list_objects_tp)

        # list 1000 objects from Bolt, num_iter times.
        for x in range(num_iter):
            list_objects_v2_start_time = time.time()
            bolt_resp = self._bolts3_client.list_objects_v2(Bucket=bucket)
            list_objects_v2_end_time = time.time()
            # calc latency
            list_objects_v2_time = list_objects_v2_end_time - list_objects_v2_start_time
            bolt_list_objects_v2_times.append(list_objects_v2_time)
            # calc throughput
            list_objects_tp = bolt_resp['KeyCount'] / list_objects_v2_time
            bolt_list_objects_v2_tp.append(list_objects_tp)

        # calc s3 perf stats
        s3_list_objects_v2_perf_stats = self._compute_perf_stats(s3_list_objects_v2_times,
                                                                 s3_list_objects_v2_tp)

        # calc bolt perf stats
        bolt_list_objects_v2_perf_stats = self._compute_perf_stats(bolt_list_objects_v2_times,
                                                                   bolt_list_objects_v2_tp)

        return {
            's3_list_objects_v2': s3_list_objects_v2_perf_stats,
            'bolt_list_objects_v2': bolt_list_objects_v2_perf_stats
        }

    def _all_perf(self, bucket):
        """
        Measures PUT,GET,DELETE,List Objects performance (latency, throughput) of Bolt / S3.
        :param bucket: bucket name
        :return: Object performance statistics
        """
        # PUT / DELETE Objects using generated key names.
        put_obj_perf_stats = self._put_object_perf(bucket)
        del_obj_perf_stats = self._delete_object_perf(bucket)

        # LIST / GET Objects on existing objects.
        list_objects_v2_perf_stats = self._list_objects_v2_perf(bucket)

        # Get the list of objects before get_obj_perf_test.
        self._keys = self._list_objects_v2(bucket)
        get_obj_perf_stats = self._get_object_perf(bucket)

        return self._merge_perf_stats(put_obj_perf_stats,
                                      get_obj_perf_stats,
                                      del_obj_perf_stats,
                                      list_objects_v2_perf_stats)

    def _merge_perf_stats(self, *perf_stats):
        """
        Merge one or more dictionaries containing
        performance statistics into one dictionary.
        :param perf_stats: one or more performance statistics
        :return: merged performance statistics
        """
        merged_perf_stats = {}
        for perf_stat in perf_stats:
            merged_perf_stats.update(perf_stat)
        return merged_perf_stats

    def _compute_perf_stats(self, op_times, op_tp=None, obj_sizes=None):
        """
        Compute performance statistics
        :param op_times: list of latencies
        :param op_tp: list of throughputs
        :param obj_sizes: list of object sizes
        :return: performance statistics (latency, throughput, object size)
        """
        # calc op latency perf.
        op_avg_time = mean(op_times)
        op_time_p50 = median_low(op_times)
        op_times.sort()
        p90_index = int(len(op_times) * 0.9)
        op_time_p90 = op_times[p90_index]

        # calc op throughout perf.
        if op_tp:
            op_avg_tp = mean(op_tp)
            op_tp_p50 = median_low(op_tp)
            op_tp.sort()
            p90_index = int(len(op_tp) * 0.9)
            op_tp_p90 = op_tp[p90_index]
            tp_perf_stats = {
                'average': "{:.2f} objects/sec".format(op_avg_tp),
                'p50': "{:.2f} objects/sec".format(op_tp_p50),
                'p90': "{:.2f} objects/sec".format(op_tp_p90)
            }
        else:
            tp = len(op_times) / math.fsum(op_times)
            tp_perf_stats = "{:.2f} objects/sec".format(tp)

        # calc obj size metrics.
        if obj_sizes:
            obj_avg_size = mean(obj_sizes)
            obj_sizes_p50 = median_low(obj_sizes)
            obj_sizes.sort()
            p90_index = int(len(obj_sizes) * 0.9)
            obj_sizes_p90 = obj_sizes[p90_index]
            obj_sizes_perf_stats = {
                'average': "{:.2f} bytes".format(obj_avg_size),
                'p50': "{:.2f} bytes".format(obj_sizes_p50),
                'p90': "{:.2f} bytes".format(obj_sizes_p90)
            }

        perf_stats = {
            'latency': {
                'average': "{:.2f} secs".format(op_avg_time),
                'p50': "{:.2f} secs".format(op_time_p50),
                'p90': "{:.2f} secs".format(op_time_p90)
            },
            'throughput': tp_perf_stats
        }
        if obj_sizes:
            perf_stats['object_size'] = obj_sizes_perf_stats

        return perf_stats

    def _generate_key_names(self, num_objects):
        """
        Generate Object names to be used in PUT/GET Object operations.
        :param num_objects: number of objects
        :return: list of object names
        """
        objects = []
        for x in range(num_objects):
            obj_name = 'bolt-s3-perf' + str(x)
            objects.append(obj_name)
        return objects

    def _generate(self, characters=string.ascii_lowercase, length=10):
        """
        Generate a random string of certain length
        :param characters: character set to be used
        :param length: length of the string.
        :return: generated string
        """
        return ''.join(random.choice(characters) for _ in range(length))

    def _list_objects_v2(self, bucket):
        """
        Returns a list of 1000 objects from the given bucket in Bolt/S3
        :param bucket: bucket name
        :return: list of first 1000 objects
        """
        resp = self._s3_client.list_objects_v2(Bucket=bucket, MaxKeys=self.NUM_KEYS)
        objects = [item['Key'] for item in resp['Contents']]
        return objects
