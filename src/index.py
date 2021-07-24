"""Index.py contais functions for creating an inverted index."""

from collections import defaultdict
from typing import Iterator, List, Tuple
import itertools
from datetime import datetime
import string

import boto3

from src import aws

# Create a mapping from words to unique IDs
# ~470,000 words in English, should be fine to store in memory
WORD_ID_MAP: defaultdict[str, int] = defaultdict(itertools.count().__next__)

# Type alias for clarity
WordFilePair = tuple[int, int]


def get_text_word_ids(text: str) -> Iterator[int]:
    """Get all the word IDs from a given text.
    Punctuation is removed from the text, case is ignored.

    Stopwords are not removed, this was not mentioned in the spec.

    Args:
        text: Some string to get word IDs for

    Returns:
        iterator of all word IDs in text
    """
    for word in text.split():
        if word == "":
            continue
        # Remove punctuation, make string uniform
        word = word.translate(str.maketrans(
            "", "", string.punctuation)).lower()
        yield WORD_ID_MAP[word]


def get_word_file_pairs(file_id: int, text: str) -> Iterator[WordFilePair]:
    """Given a file-id its text contents, return an iterator with file-id
    and word-id pairs.

    Args:
        file_id: identifier for file
        text: text contents of file

    Returns:
        iterator of (word-id, file-id) pairs
    """
    file_id_it = itertools.repeat(file_id)
    word_id_it = get_text_word_ids(text)
    return zip(word_id_it, file_id_it)


def create_inverted_index(
    file_line_it: Iterator[Tuple[int, str]]
) -> defaultdict[int, List[int]]:
    """Given an iterator producing pairs of (<file-id>, <line-from-file>), produce
    an inverted index containing a mapping for each word ID to the set of all file IDs
    that contain that word.

    Args:
        file_line_it: Iterator producing consecutive pairs of
        (<file-id>, <line-from-file>)

    Returns:
        mapping of word IDs to set of all file IDs that contain that word
          i.e. <word-id>: <all-associated-file-ids>
    """
    word_file_map = defaultdict(list)
    for fname, line in file_line_it:
        word_file_it = get_word_file_pairs(fname, line)
        for word_id, file_id in word_file_it:
            word_file_map[word_id].append(file_id)

    # Sorting once complete is faster than maintaining a sorted list with bisect
    for word_id in word_file_map.keys():
        word_file_map[word_id].sort()

    return word_file_map


def main():
    s3 = boto3.resource("s3")

    # Iterate through all lines in all S3 files, get (<word-id>,<file-id>) pairs
    file_text_it = aws.iterate_text_pairs(s3, aws.BUCKET)
    inverted_index = create_inverted_index(file_text_it)

    dt_now = datetime.now().strftime("%Y-%m-%d--%H:%M")
    aws.write_index(s3, aws.BUCKET, f"index--{dt_now}", inverted_index)
    # Writing the word-id map is not mentioned in spec, but it's obviously required
    aws.write_index(s3, aws.BUCKET, f"word-mapping--{dt_now}", WORD_ID_MAP)
