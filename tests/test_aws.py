import unittest
import json

import boto3
from moto import mock_s3

from src import aws


class S3TestCase(unittest.TestCase):
    mock_s3 = mock_s3()
    bucket = "SEBSTRUG_TEST"

    def setUp(self):
        self.mock_s3.start()

        # you can use boto3.client('s3') if you prefer
        self.s3 = boto3.resource("s3")
        bucket = self.s3.Bucket(self.bucket)
        bucket.create(CreateBucketConfiguration={
                      "LocationConstraint": aws.REGION})

    def tearDown(self):
        self.mock_s3.stop()


class TestIterateTextPairs(S3TestCase):
    def setUp(self):
        super().setUp()
        object = self.s3.Object(self.bucket, "1.txt")
        object.put(Body="foo\nbar")

        object = self.s3.Object(self.bucket, "2.txt")
        object.put(Body="baz")

    def test_multiple_objects(self):
        it = aws.iterate_text_pairs(self.s3, self.bucket)
        self.assertEqual((1, b"foo"), next(it))
        self.assertEqual((1, b"bar"), next(it))
        self.assertEqual((2, b"baz"), next(it))

        # Make sure we don't keep returning s3 keys with no content!
        with self.assertRaises(StopIteration):
            next(it)


class TestWriteIndex(S3TestCase):
    def test(self):
        input_dict = {"1": ["1", "2", "3"], "2": ["2", "3"]}
        aws.write_index(self.s3, self.bucket, "index.json", input_dict)
        body = self.s3.Object(self.bucket, "index.json").get()["Body"]
        self.assertEqual(input_dict, json.load(body))
