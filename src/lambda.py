from typing import Dict

import boto3

from handler import AWSHandler


def lambda_handler(event: Dict[str, str], context):
    s3 = boto3.resource("s3")
    handler = AWSHandler(s3)

    s3_key = event.get("key")
    quote = handler.load_object(s3_key)
    return {"quote": quote}
