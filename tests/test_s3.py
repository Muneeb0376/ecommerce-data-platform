import pandas as pd
import pytest

from src.loading.s3 import build_key, upload_dataframe, upload_file


class FakeS3:
    """Records put_object calls instead of talking to AWS."""

    def __init__(self):
        self.objects = {}

    def put_object(self, Bucket, Key, Body, **kwargs):
        self.objects[(Bucket, Key)] = Body


def test_build_key_partitions_by_day_and_run():
    key = build_key("processed", "price_updates", "20261010T120000", "clean.csv")
    assert key == "processed/price_updates/dt=2026-10-10/run_id=20261010T120000/clean.csv"


@pytest.mark.parametrize("bad", ["", "abc", "2026", "2026-10-10"])
def test_build_key_rejects_bad_run_id(bad):
    with pytest.raises(ValueError):
        build_key("raw", "x", bad, "f.csv")


def test_same_run_id_gives_same_key():
    a = build_key("raw", "price_updates", "20261010T120000", "f.csv")
    b = build_key("raw", "price_updates", "20261010T120000", "f.csv")
    assert a == b


def test_upload_dataframe_writes_csv_without_index():
    s3 = FakeS3()
    df = pd.DataFrame({"product_id": [1, 2], "new_price": [10.5, 20.0]})
    upload_dataframe(df, "processed/x.csv", client=s3, bucket="my-bucket")
    body = s3.objects[("my-bucket", "processed/x.csv")].decode("utf-8")
    assert body.splitlines() == ["product_id,new_price", "1,10.5", "2,20.0"]


def test_upload_dataframe_empty_frame_keeps_header():
    s3 = FakeS3()
    df = pd.DataFrame(columns=["product_id", "new_price"])
    upload_dataframe(df, "k.csv", client=s3, bucket="b")
    assert s3.objects[("b", "k.csv")].decode("utf-8").strip() == "product_id,new_price"


def test_upload_file(tmp_path):
    f = tmp_path / "data.csv"
    f.write_bytes(b"a,b\n1,2\n")  # bytes, so Windows does not turn \n into \r\n
    s3 = FakeS3()
    upload_file(f, "raw/data.csv", client=s3, bucket="b")
    assert s3.objects[("b", "raw/data.csv")] == b"a,b\n1,2\n"


def test_bucket_must_be_configured(monkeypatch):
    from src.loading.s3 import get_bucket

    monkeypatch.delenv("S3_BUCKET", raising=False)
    with pytest.raises(RuntimeError):
        get_bucket()