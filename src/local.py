from abc import abstractmethod
from typing import Any, Iterator, Tuple, Dict
from datetime import datetime
from pathlib import Path
import json
import itertools


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
        for fname in Path(self.local_path).glob('*.txt'):
            file_id = int(fname.stem)
            file_id_iterator = itertools.repeat(file_id)
            with fname.open('r') as f:
                yield from zip(file_id_iterator, f.read().splitlines())

    def write_index(self, index: Dict):
        dt_str = datetime.now().strftime('%Y-%m-%d--%H:%M')
        path = Path(self.local_path) / f'index-{dt_str}.json'
        with path.open('w') as f:
            json.dump(index, f)
