"""Generate inverted index using AWS"""

from typing import Dict

import boto3

from index import create_inverted_index, WORD_ID_MAP
from handler import AWSHandler


def lambda_handler(event: Dict[str, str], context):
    s3 = boto3.resource("s3")
    handler = AWSHandler(s3)

    file_text_it = handler.iterate_text_pairs()
    inverted_index = create_inverted_index(file_text_it)
    handler.write_index("index", inverted_index)
    handler.write_index("word-ids", WORD_ID_MAP)
    return True
