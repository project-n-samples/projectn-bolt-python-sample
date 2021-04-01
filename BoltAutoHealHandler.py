import bolt as bolt3
import time


def lambda_handler(event, context):
    bucket = event['bucket']
    key = event['key']

    # Bolt Client.
    bolts3_client = bolt3.client('s3')

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
