import bolt as bolt3
import time


def lambda_handler(event, context):
    """
    lambda_handler is the handler function that is invoked by AWS Lambda to process an incoming event for
    performing auto-heal tests.

    lambda_handler accepts the following input parameters as part of the event:
    1) bucket - bucket name
    2) key - key name

    :param event: incoming event data
    :param context: runtime information
    :return: time taken to auto-heal
    """
    bucket = event['bucket']
    key = event['key']

    # Bolt Client.
    bolts3_client = bolt3.client('s3')

    # Attempt to retrieve object repeatedly until it succeeds, which would indicate successful
    # auto-healing of the object.
    auto_heal_start_time = time.time()
    while True:
        try:
            # Get Object from Bolt
            bolt_resp = bolts3_client.get_object(Bucket=bucket, Key=key)
            # read all the data from StreamingBody.
            # bolt_resp['Body'].read()
            # exit on success after auto-heal
            auto_heal_end_time = time.time()
            break
        except Exception as e:
            pass

    auto_heal_time = auto_heal_end_time - auto_heal_start_time

    return {
        'auto_heal_time': "{:.2f} secs".format(auto_heal_time)
    }
