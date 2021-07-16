from pathlib import Path
import json

from src.local import LocalIndexHandler


def test_iterate_text_pairs(tmp_path):
    path_1 = tmp_path / "1.txt"
    quote_1 = "'Some quote'"
    origin_1 = 'Author'
    path_1.write_text('\n'.join((quote_1, origin_1)))

    path_2 = tmp_path / "2.txt"
    quote_2 = "'Some other quote'"
    origin_2 = 'Musician'
    path_2.write_text('\n'.join((quote_2, origin_2)))

    index_handler = LocalIndexHandler(tmp_path)
    res_it = index_handler.iterate_text_pairs()

    expected_res = [(1, quote_1), (1, origin_1), (2, quote_2), (2, origin_2)]
    res = []
    while True:
        try:
            res.append(next(res_it))
        except StopIteration:
            break

    assert not (set(expected_res) ^ set(res))


def test_write_index(tmp_path):
    index_handler = LocalIndexHandler(tmp_path)
    index = {'a': 1, 'b': 2}
    index_handler.write_index(index)

    index_glob = Path(tmp_path).glob("index*.json")
    index_files = list(index_glob)
    assert len(index_files) == 1
    with index_files[0].open('r') as f:
        res_index = json.load(f)
    assert res_index == index
