from BoltS3Perf import BoltS3Perf


def lambda_handler(event, context):
    """
    lambda_handler is the handler function that is invoked by AWS Lambda to process an incoming event
    for Bolt/S3 Performance testing

    lambda_handler accepts the following input parameters as part of the event:
    1) requestType - type of request / operation to be performed. The following requests are supported:
       a) list_objects_v2 - list objects
       b) get_object - get object
       c) get_object_passthrough - get object (via passthrough) of unmonitored bucket
       d) put_object - upload object
       e) delete_object - delete object
       f) all - put, get, delete, list objects (default request if none specified)

    2) bucket - bucket name

    Following are examples of events, for various requests, that can be used to invoke the handler function.
    a) Measure List objects performance of Bolt / S3.
       {"requestType": "list_objects_v2", "bucket": "<bucket>"}

    b) Measure Get object performance of Bolt / S3.
       {"requestType": "get_object", "bucket": "<bucket>"}

    c) Measure Get object passthrough performance of Bolt.
       {"requestType": "get_object_passthrough", "bucket": "<unmonitored-bucket>"}

    d) Measure Put object performance of Bolt / S3.
       {"requestType": "put_object", "bucket": "<bucket>"}

    e) Measure Delete object performance of Bolt / S3.
       {"requestType": "delete_object", "bucket": "<bucket>"}

    f) Measure Put, Delete, Get, List objects performance of Bolt / S3.
       {"requestType": "all", "bucket": "<bucket>"}

    :param event: incoming event data
    :param context: runtime information
    :return: response from BoltS3Perf
    """
    bolts3_perf = BoltS3Perf()
    return bolts3_perf.process_event(event)
