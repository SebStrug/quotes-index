"""Generate inverted index using AWS"""
from datetime import datetime

import boto3

from index import create_inverted_index, WORD_ID_MAP
from handler import AWSHandler


def lambda_handler(event: None, context):
    s3 = boto3.resource("s3")
    handler = AWSHandler(s3)

    file_text_it = handler.iterate_text_pairs()
    inverted_index = create_inverted_index(file_text_it)

    dt_str = datetime.now().strftime("%Y-%m-%d--%H:%M")
    handler.write_index(f"index-{dt_str}", inverted_index)
    handler.write_index(f"word-ids-{dt_str}", WORD_ID_MAP)
