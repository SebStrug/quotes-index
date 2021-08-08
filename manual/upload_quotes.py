from pathlib import Path
import os
import logging
import sys

import boto3

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger(__name__)


def main():
    bucket = os.getenv("QUOTES_INDEX_S3_BUCKET")
    s3_client = boto3.client("s3")
    quotes_files = list((Path(__file__).parent.parent / "quotes").glob("*txt"))
    for ind, f in enumerate(quotes_files, start=1):
        _ = s3_client.upload_file(f.as_posix(), bucket, f.name)
        if ind % 10 == 0:
            logger.info(f"Uploaded {ind} files")


if __name__ == "__main__":
    main()
