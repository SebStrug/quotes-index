"""Generate inverted index using localhost"""

from pathlib import Path

from src.index import create_inverted_index, WORD_ID_MAP
from src.handler import LocalHandler


def main():
    quotes_path = Path(__file__).parent.parent / "quotes"
    handler = LocalHandler(quotes_path)

    file_text_it = handler.iterate_text_pairs()
    inverted_index = create_inverted_index(file_text_it)
    handler.write_index("index", inverted_index)
    handler.write_index("word-ids", WORD_ID_MAP)


if __name__ == "__main__":
    main()
