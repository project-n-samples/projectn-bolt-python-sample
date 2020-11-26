from BoltS3OpsClient import BoltS3OpsClient


def lambda_handler(event, context):
    bolts3_ops_client = BoltS3OpsClient()
    return bolts3_ops_client.process_event(event)
