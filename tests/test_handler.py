from pathlib import Path
import json
import os

import pytest
import boto3
from moto import mock_s3

from src.handler import LocalHandler, AWSHandler, form_quote


def test_form_quote():
    input = {
        "content": "some other quote",
        "lead_in": None,
        "attribution": "Seb",
        "source": "This test",
    }
    expected = "'some other quote'\nSeb, This test"
    assert expected == form_quote(input.pop("content"), **input)


def test_local_iterate_text_pairs(tmp_path):
    path_1 = tmp_path / "1.txt"
    quote_1 = "'Some quote'"
    origin_1 = "Author"
    path_1.write_text("\n".join((quote_1, origin_1)))

    path_2 = tmp_path / "2.txt"
    quote_2 = "'Some other quote'"
    origin_2 = "Musician"
    path_2.write_text("\n".join((quote_2, origin_2)))

    index_handler = LocalHandler(tmp_path)
    res_it = index_handler.iterate_text_pairs()

    expected_res = [(1, quote_1), (1, origin_1), (2, quote_2), (2, origin_2)]
    res = []
    while True:
        try:
            res.append(next(res_it))
        except StopIteration:
            break

    assert not (set(expected_res) ^ set(res))


def test_local_write_index(tmp_path):
    index_handler = LocalHandler(tmp_path)
    index = {"a": 1, "b": 2}
    index_handler.write_index("index", index)

    index_glob = Path(tmp_path).glob("index*.json")
    index_files = list(index_glob)
    assert len(index_files) == 1
    with index_files[0].open("r") as f:
        res_index = json.load(f)
    assert res_index == index


def test_local_load_index(tmp_path):
    obj = {"a": 1, "b": 2}
    path = Path(tmp_path) / "test_file.json"
    with open(path, "w") as f:
        json.dump(obj, f)

    index_handler = LocalHandler(tmp_path)
    data = index_handler.load_index("test")
    assert data == obj


def test_local_add_quote(tmp_path):
    initial_quote = "'Some quote\nSeb, This Test'"
    initial_quote_path = Path(tmp_path) / "1.txt"
    with open(initial_quote_path, "w") as f:
        f.write(initial_quote)

    quote_to_add = {
        "content": "some other quote",
        "lead_in": None,
        "attribution": "Seb",
        "source": "This test",
    }
    handler = LocalHandler(tmp_path)
    handler.add_quote(**quote_to_add)

    expected_path = Path(tmp_path / "2.txt")
    print(list(Path(tmp_path).glob("*")))
    assert expected_path.exists()
    with open(expected_path, "r") as f:
        res = f.read()
    assert "'some other quote'\nSeb, This test\n" == res


@pytest.fixture
def s3_resource():
    with mock_s3():
        s3 = boto3.resource("s3")
        yield s3


@pytest.fixture
def bucket_name():
    return "sebstrug-test"


@pytest.fixture
def region():
    return "eu-west-1"


@pytest.fixture
def aws_credentials(bucket_name, region):
    """Mocked AWS Credentials for moto."""
    os.environ["QUOTES_INDEX_S3_BUCKET"] = bucket_name
    os.environ["S3_BUCKET"] = bucket_name
    os.environ["AWS_REGION"] = region


@pytest.fixture
def s3_test(s3_resource, bucket_name, region, aws_credentials):
    bucket = s3_resource.Bucket(bucket_name)
    bucket.create(CreateBucketConfiguration={"LocationConstraint": region})
    yield


@pytest.fixture
def s3_index(s3_resource, bucket_name, s3_test):
    """Preload index onto S3"""
    object = s3_resource.Object(bucket_name, "index_test.json")
    object.put(Body=json.dumps({"1": ["1", "2", "3"], "2": ["2", "3"]}))
    object = s3_resource.Object(bucket_name, "index_zzz.json")
    object.put(Body=json.dumps({"1": ["1", "2", "3"], "2": ["2", "3"]}))
    yield


def test_aws_iterate_text_pairs(s3_resource, bucket_name, s3_test):
    object = s3_resource.Object(bucket_name, "1.txt")
    object.put(Body="foo\nbar")

    object = s3_resource.Object(bucket_name, "2.txt")
    object.put(Body="baz")

    index_handler = AWSHandler(s3_resource)

    it = index_handler.iterate_text_pairs()
    assert (1, b"foo") == next(it)
    assert (1, b"bar") == next(it)
    assert (2, b"baz") == next(it)

    # Make sure we don't keep returning s3 keys with no content!
    with pytest.raises(StopIteration) as _:
        next(it)


def test_aws_write_index(s3_resource, bucket_name, s3_test):
    input_dict = {"1": ["1", "2", "3"], "2": ["2", "3"]}

    index_handler = AWSHandler(s3_resource)
    index_handler.write_index("index.json", input_dict)

    body = s3_resource.Object(bucket_name, "index.json").get()["Body"]
    assert input_dict == json.load(body)


def test_aws_load_index(s3_resource, bucket_name, s3_test, s3_index):
    index_handler = AWSHandler(s3_resource)
    index = index_handler.load_index("index_test.json")
    assert index == {"1": ["1", "2", "3"], "2": ["2", "3"]}


def test_aws_load_latest_index(s3_resource, bucket_name, s3_test, s3_index):
    index_handler = AWSHandler(s3_resource)
    index = index_handler.load_index("index")
    assert index == {"1": ["1", "2", "3"], "2": ["2", "3"]}


def test_aws_add_quote(s3_resource, bucket_name, s3_test):
    index_handler = AWSHandler(s3_resource)
    index_handler.add_quote(content="Test quote")

    bucket = s3_resource.Bucket(bucket_name)
    for f in bucket.objects.all():
        obj = f.get()
    assert obj["Body"].read().decode() == "'Test quote'\nAnonymous"
