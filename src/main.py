"""Generate an inverted index.

Use local files:
$ python -m src.main --source local

Use AWS:
$ python -m src.main --source aws
"""

import argparse
from pathlib import Path

import boto3

from src.index import create_inverted_index, WORD_ID_MAP
from src.local import LocalIndexHandler


def parse_args():
    parser = argparse.ArgumentParser()
    # local or remote/aws
    parser.add_argument("--source")
    return parser


def main():
    parser = parse_args()
    args = parser.parse_args()

    if args.source == "local":
        quotes_path = Path(__file__).parent.parent / "quotes"
        index_handler = LocalIndexHandler(quotes_path)
    elif args.source == "aws":
        _ = boto3.resource("s3")
        ...

    file_text_it = index_handler.iterate_text_pairs()
    inverted_index = create_inverted_index(file_text_it)
    index_handler.write_index("index", inverted_index)
    index_handler.write_index("word-ids", WORD_ID_MAP)


if __name__ == "__main__":
    main()
