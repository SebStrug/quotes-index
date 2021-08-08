import unittest
from collections import defaultdict
import itertools

from src import index


class BasicTestCase(unittest.TestCase):
    """Base test class to override the module level variable
    WORD_ID_MAP for each test
    """

    def setUp(self):
        index.WORD_ID_MAP = defaultdict(itertools.count().__next__)


class TestWordToId(BasicTestCase):
    def test(self):
        self.assertEqual(0, index.WORD_ID_MAP["foo"])
        self.assertEqual(1, index.WORD_ID_MAP["bar"])
        self.assertEqual(0, index.WORD_ID_MAP["foo"])
        self.assertEqual(0, index.WORD_ID_MAP["foo"])
        self.assertEqual(2, index.WORD_ID_MAP["baz"])


class TestGetTextWordIds(BasicTestCase):
    def test_empty(self):
        it = index.get_text_word_ids("")
        with self.assertRaises(StopIteration):
            next(it)

    def test(self):
        it = index.get_text_word_ids("this is a line index this line")
        for ind in range(0, 5):
            self.assertEqual(ind, next(it))

    def test_dirty(self):
        it = index.get_text_word_ids('This is a line. Index this "line"')
        for ind in range(0, 5):
            self.assertEqual(ind, next(it))


class TestGetWordFilePairs(BasicTestCase):
    def test_empty(self):
        it = index.get_word_file_pairs(1, "")
        with self.assertRaises(StopIteration):
            next(it)

    def test(self):
        it = index.get_word_file_pairs(1, "this is a line")
        for ind in range(4):
            self.assertEqual((ind, 1), next(it))

    def test_multiline(self):
        it = index.get_word_file_pairs(
            2,
            """this is a line
this is another line""",
        )
        self.assertEqual((0, 2), next(it))
        self.assertEqual((1, 2), next(it))
        self.assertEqual((2, 2), next(it))
        self.assertEqual((3, 2), next(it))
        self.assertEqual((0, 2), next(it))
        self.assertEqual((1, 2), next(it))
        self.assertEqual((4, 2), next(it))
        self.assertEqual((3, 2), next(it))


class TestCreateInvertedIndex(BasicTestCase):
    def test(self):
        file_line_it = iter(
            (
                (4, "this is a file containing some lines"),
                (1, "this is a file containing some lines"),
                (2, "yet more lines here"),
                (3, "here is another file"),
            )
        )

        expected_output = {
            0: [1, 4],
            1: [1, 3, 4],
            2: [1, 4],
            3: [1, 3, 4],
            4: [1, 4],
            5: [1, 4],
            6: [1, 2, 4],
            7: [2],
            8: [2],
            9: [2, 3],
            10: [3],
        }
        self.assertDictEqual(
            expected_output, dict(index.create_inverted_index(file_line_it))
        )
