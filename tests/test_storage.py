import pytest


def test_target(tmp_target):
    mapper = tmp_target.get_mapper()
    mapper["foo"] = b"bar"
    with open(tmp_target.root_path + "/foo") as f:
        res = f.read()
    assert res == "bar"
    with pytest.raises(FileNotFoundError):
        tmp_target.rm("baz")
    with pytest.raises(FileNotFoundError):
        with tmp_target.open("baz"):
            pass


def test_cache(tmp_cache):
    assert not tmp_cache.exists("foo")
    with tmp_cache.open("foo", mode="w") as f:
        f.write("bar")
    assert tmp_cache.exists("foo")
    assert tmp_cache.size("foo") == 3
    with tmp_cache.open("foo", mode="r") as f:
        assert f.read() == "bar"
    tmp_cache.rm("foo")
    assert not tmp_cache.exists("foo")


def test_metadata_target(tmp_metadata_target):
    data = {"foo": 1, "bar": "baz"}
    tmp_metadata_target["key1"] = data
    assert tmp_metadata_target["key1"] == data
    assert tmp_metadata_target.getitems(["key1"]) == {"key1": data}
