from abc import abstractmethod
from typing import Any, Iterator, Tuple, Dict
from datetime import datetime
from pathlib import Path
import json
import itertools
import os
import logging

from botocore.exceptions import BotoCoreError

logger = logging.getLogger(__name__)


class IndexHandler:
    """Interface for loading files into iterator of file IDs and lines"""

    @abstractmethod
    def iterate_text_pairs(self, *args: Any) -> Iterator[Tuple[int, str]]:
        """Generate successive pairs of (<file id>, <line from file>) tuples
        where the file ID is a consecutive integer
        """
        raise NotImplementedError

    @abstractmethod
    def write_index(self, *args: Any):
        """Write a dictionary containing an inverted index to path"""
        raise NotImplementedError

    @abstractmethod
    def load_index(self, *args: Any):
        """Load in an dictionary containing an inverted index from path"""


class LocalIndexHandler(IndexHandler):
    """Interact with local files to handle index"""

    def __init__(self, local_path: Path):
        """
        Args:
            local_path: Path containing quotes as consecutive text files.
            Index will also be saved to local path
        """
        self.local_path = local_path

    def iterate_text_pairs(self):
        for fname in Path(self.local_path).glob("*.txt"):
            file_id = int(fname.stem)
            file_id_iterator = itertools.repeat(file_id)
            with fname.open("r") as f:
                yield from zip(file_id_iterator, f.read().splitlines())

    def write_index(self, prefix: str, index: Dict):
        """Write a dictionary to a file with the filename as
        <path>/<prefix>-YYYY-MM-DD--HH:MM.json
        """
        dt_str = datetime.now().strftime("%Y-%m-%d--%H:%M")
        path = Path(self.local_path) / f"{prefix}-{dt_str}.json"
        with path.open("w") as f:
            json.dump(index, f)

    def load_index(self, prefix: str) -> Dict:
        """Searches the local path for a prefix, loads in the latest
        associated object from JSON as a dictionary.
        """
        objs = list(Path(self.local_path).glob(f"{prefix}*.json"))
        if not objs:
            raise FileNotFoundError(f"No objects with prefix: {prefix}")
        with objs[-1].open("r") as f:
            data = json.load(f)
        return data


class AWSIndexHandler(IndexHandler):
    def __init__(self):
        self.bucket = os.getenv("S3_BUCKET")
        self.region = os.getenv("S3_REGION", "eu-west-1")

    def iterate_text_pairs(self, s3_res: Any, bucket: str) -> Iterator[Tuple[int, str]]:
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

    def write_index(self, s3_res: Any, bucket: str, s3_key: str, index: Dict):
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
            object.put(Body=json.dumps(index))
        except BotoCoreError:
            logger.error(
                f"Failed to upload dictionary to key: {s3_key}", exc_info=True)
            raise
