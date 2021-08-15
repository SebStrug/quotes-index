from abc import abstractmethod
from typing import Any, Iterator, Tuple, Dict, Optional
from datetime import datetime
from pathlib import Path
import json
import itertools
import re
import os
import logging
import sys

from botocore.exceptions import BotoCoreError

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger(__name__)


def form_quote(
    content: str,
    lead_in: Optional[str] = None,
    source: str = "Anonymous",
) -> str:
    """Create a quote out of several provided fields

    Args:
        content: body of the quote
        attribution: who said/wrote the quote. Defaults to 'Anonymous'
        lead_in: initial/background info on quote e.g. 'On the meaning of life'
        source: Origin of the quote

    Returns:
        Quote of the form
        ```
        <lead_in>...
        '<content>'
        <source>
    """
    if not content:
        raise ValueError("No quote provided")

    lead_in = lead_in + "..." if lead_in else ""
    return "\n".join((lead_in, f"'{content}'", source)).strip()


class Handler:
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
    def load_object(self, *args: Any):
        """Load in any object from the environment"""
        raise NotImplementedError

    @abstractmethod
    def load_index(self, *args: Any):
        """Load in an dictionary containing an inverted index from path"""
        raise NotImplementedError

    @abstractmethod
    def add_quote(self, *args: Any, **kwargs: Any):
        """Add a quote, do NOT update the index"""
        raise NotImplementedError


class LocalHandler(Handler):
    """Interact with local files to handle index"""

    def __init__(self, local_path: Path):
        """
        Args:
            local_path: Path containing quotes as consecutive text files.
            Index will also be saved to local path
        """
        self.local_path = local_path

    def iterate_text_pairs(self) -> Iterator[Tuple[int, str]]:
        for fname in Path(self.local_path).glob("*.txt"):
            file_id = int(fname.stem)
            logger.debug(f"Found file ID: {file_id}")
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
        logger.info(f"Wrote dictionary to path: {path}")

    def load_object(self, prefix: str) -> str:
        """Load an object by prefix from local path.
        If several objects have the same prefix, load the
        last object
        """
        objs = Path(self.local_path).glob(f"{prefix}*")
        for obj in objs:
            pass
        try:
            with obj.open("r") as f:
                data = f.read()
        except NameError:
            logging.error(f"No object with prefix: {prefix}")
            return ""
        return data

    def load_index(self, prefix: str) -> Dict:
        """Searches the local path for a prefix, loads in the latest
        associated object from JSON as a dictionary.
        """
        data_str = self.load_object(prefix)
        return json.loads(data_str)

    def add_quote(self, **kwargs):
        last_quote_index = list(self.local_path.glob("*.txt"))[-1].stem
        next_quote_index = int(last_quote_index.rstrip(".txt")) + 1
        next_quote_fname = self.local_path / f"{next_quote_index}.txt"

        quote = form_quote(kwargs.pop("content"), **kwargs)
        with open(next_quote_fname, "w") as f:
            f.write(quote + "\n")


class AWSHandler(Handler):
    def __init__(self, s3_res: Any):
        """
        Args:
            s3_res: instantiated s3 resource object
        """
        self.bucket = os.getenv("QUOTES_INDEX_S3_BUCKET")
        self.region = os.getenv("QUOTES_INDEX_AWS_REGION", "eu-west-1")
        self.s3_res = s3_res

    def iterate_text_pairs(self) -> Iterator[Tuple[int, str]]:
        """Generate successive pairs of (<s3-key>, <line-from-s3-file>) tuples from files
        in a s3 bucket which are labeled by their order in the bucket.
        i.e. '1.txt', '2.txt', ...

        Returns:
            iterator yielding (<file-id>, <line-from-file>) pairs
        """
        bucket = self.s3_res.Bucket(self.bucket)
        logger.info(f"Iterating through items in bucket: {bucket}")

        for f in bucket.objects.all():
            if not re.match(r"\d+.txt", f.key):
                continue

            # Assume s3 keys are named by order in bucket, as per spec
            file_id = int(Path(f.key).stem)
            logger.debug(f"Found file ID: {file_id}")
            file_id_iterator = itertools.repeat(file_id)

            try:
                obj = f.get()
            except BotoCoreError:
                logger.error(f"Failed to get s3 object: {f.key}")

            yield from zip(file_id_iterator, obj["Body"].iter_lines())

    def write_index(self, s3_key: str, index: Dict):
        """Write an dictionary to a s3 path

        Args:
            s3_res: instantiated s3 resource object
            bucket: name of bucket to write to
            s3_key: key to s3 path to write to, including filename
            e.g. `index_20210527.json`
            index: dictionary to write to S3
        """
        if not index:
            return

        try:
            object = self.s3_res.Object(self.bucket, s3_key)
            object.put(Body=json.dumps(index))
            logger.info(f"Wrote dictionary to key: {s3_key}")
        except BotoCoreError:
            logger.error(f"Failed to upload dictionary to key: {s3_key}", exc_info=True)
            raise

    def load_object(self, s3_key: str) -> str:
        """Serve the data in an S3 object

        Args:
            s3_key: s3 path to read from. If a prefix is used,
            load the last object
        """
        bucket = self.s3_res.Bucket(self.bucket)
        for obj in bucket.objects.filter(Prefix=s3_key):
            object = obj

        try:
            # object = self.s3_res.Object(self.bucket, s3_key)
            response = object.get()
            return response["Body"].read()
        except BotoCoreError:
            logger.error(f"Failed to load object from key: {s3_key}", exc_info=True)
            raise

    def load_index(self, s3_key: str) -> Dict:
        """Load an index, or any dictionary from an S3 object

        Args:
            s3_key: s3 path to read from. If a prefix is passed,
            load the last object.
        """
        try:
            object = self.load_object(s3_key)
            return json.loads(object)
        except BotoCoreError:
            logger.error(f"Failed to load dictionary from key: {s3_key}", exc_info=True)
            raise

    def add_quote(self, **kwargs):
        tstamp = int(datetime.now().timestamp())
        s3_key = f"{tstamp}.txt"

        quote = form_quote(kwargs.pop("content"), **kwargs)
        object = self.s3_res.Object(self.bucket, s3_key)
        object.put(Body=quote)
