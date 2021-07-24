from collections.abc import Iterator
from typing import Any, Tuple, Dict
import os
import logging
from pathlib import Path
import json
import itertools

from botocore.exceptions import BotoCoreError

BUCKET = os.getenv("S3_BUCKET")
REGION = os.getenv("S3_REGION", "eu-west-1")

logger = logging.getLogger(__name__)


def iterate_text_pairs(s3_res: Any, bucket: str) -> Iterator[Tuple[int, str]]:
    """Generate successive pairs of (<s3-key>, <line-from-s3-file>) tuples from files
    in a s3 bucket which are labeled by their order in the bucket.
    i.e. '1.txt', '2.txt', ...

    Args:
        s3_res: instantiated s3 resource object
        bucket: name of bucket to access

    Returns:
        iterator yielding (<file-id>, <line-from-file>) pairs
    """
    bucket = s3_res.Bucket(bucket)
    logger.info(f"Iterating through items in bucket: {bucket}")

    for f in bucket.objects.all():
        # Assume s3 keys are named by order in bucket, as per spec
        file_id = int(Path(f.key).stem)
        file_id_iterator = itertools.repeat(file_id)

        try:
            obj = f.get()
        except BotoCoreError:
            logger.error(f"Failed to get s3 object: {f.key}")

        yield from zip(file_id_iterator, obj["Body"].iter_lines())


def write_index(s3_res: Any, bucket: str, s3_key: str, index: Dict):
    """Write an dictionary to a s3 path

    Args:
        s3_res: instantiated s3 resource object
        bucket: name of bucket to write to
        s3_key: key to s3 path to write to, including filename
          e.g. `index_20210527.json`
        index: dictionary to write to S3
    """
    if not dict:
        return

    try:
        object = s3_res.Object(bucket, s3_key)
        object.put(Body=json.dumps(dict))
    except BotoCoreError:
        logger.error(f"Failed to upload dictionary to key: {s3_key}", exc_info=True)
        raise
