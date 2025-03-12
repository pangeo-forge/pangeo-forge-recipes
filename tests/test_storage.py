import hashlib
import os

import pytest
from fsspec.implementations.http import HTTPFileSystem
from fsspec.implementations.local import LocalFileSystem
from gcsfs import GCSFileSystem
from s3fs import S3FileSystem

from pangeo_forge_recipes.storage import CacheFSSpecTarget, FSSpecTarget

POSIX_MAX_FNAME_LENGTH = 255


async def _do_target_ops(target):
    # store = target.get_mapper()
    # await store.set("foo", b"bar")
    # with open(target.root_path + "/foo") as f:
    #     res = f.read()
    # assert res == "bar"
    with pytest.raises(FileNotFoundError):
        await target.rm("baz")
    with pytest.raises(FileNotFoundError):
        with target.open("baz"):
            pass


@pytest.mark.asyncio
async def test_target(tmp_target):
    await _do_target_ops(tmp_target)


@pytest.mark.asyncio
async def test_from_url(tmpdir_factory):
    path = str(tmpdir_factory.mktemp("target"))
    target = FSSpecTarget.from_url(path)
    await _do_target_ops(target)


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


@pytest.fixture
def fname_longer_than_posix_max():
    extension = ".nc"
    fname = "".join(["a" for i in range(POSIX_MAX_FNAME_LENGTH + 1 - len(extension))]) + extension
    assert len(fname) > POSIX_MAX_FNAME_LENGTH
    return fname, extension


def test_caching_local_fname_length_not_greater_than_255_bytes(
    tmpdir_factory,
    fname_longer_than_posix_max,
):
    tmp_path = tmpdir_factory.mktemp("fname-length-tempdir")
    fname, extension = fname_longer_than_posix_max

    obj_without_fname_len_control = FSSpecTarget(LocalFileSystem(), tmp_path)
    _, uncontrolled_fname = os.path.split(obj_without_fname_len_control._full_path(fname))
    assert len(uncontrolled_fname) > POSIX_MAX_FNAME_LENGTH
    with pytest.raises(OSError, match="File name too long"):
        with obj_without_fname_len_control.open(fname, mode="w"):
            pass

    cache_with_fname_len_control = CacheFSSpecTarget(LocalFileSystem(), tmp_path)
    _, controlled_fname = os.path.split(cache_with_fname_len_control._full_path(fname))
    assert len(controlled_fname) == POSIX_MAX_FNAME_LENGTH
    _, actual_extension = os.path.splitext(cache_with_fname_len_control._full_path(fname))
    assert actual_extension == extension
    expected_prefix = hashlib.md5(fname.encode()).hexdigest()
    assert controlled_fname.startswith(expected_prefix)
    with cache_with_fname_len_control.open(fname, mode="w"):
        pass


@pytest.mark.parametrize("fs_cls", [LocalFileSystem, HTTPFileSystem])
def test_caching_only_truncates_long_fnames_for_local_fs(fs_cls, fname_longer_than_posix_max):
    cache = CacheFSSpecTarget(fs_cls(), "root_path")
    fname, _ = fname_longer_than_posix_max

    _, fname_in_full_path = os.path.split(cache._full_path(fname))

    if fs_cls is LocalFileSystem:
        assert len(fname_in_full_path) == POSIX_MAX_FNAME_LENGTH
    else:
        assert len(fname_in_full_path) > POSIX_MAX_FNAME_LENGTH


def test_suffix(tmp_path):
    assert str((FSSpecTarget(LocalFileSystem(), tmp_path) / "test").root_path) == str(
        tmp_path / "test"
    )


@pytest.mark.parametrize("fs_cls", [LocalFileSystem, HTTPFileSystem, S3FileSystem, GCSFileSystem])
def test_target_storage_get_remote_protocol(fs_cls, monkeypatch):
    # we need to use patch here for s3fs and gcsfs b/c they try to do so much on __init__
    monkeypatch.setattr("s3fs.S3FileSystem.__init__", lambda x: None)
    monkeypatch.setattr("gcsfs.GCSFileSystem.__init__", lambda x: None)
    monkeypatch.setattr("pangeo_forge_recipes.storage.FSSpecTarget.__post_init__", lambda x: None)
    target_root = FSSpecTarget(fs_cls())
    if isinstance(target_root, LocalFileSystem):
        assert target_root.fs.get_fsspec_remote_protocol() == "local"
    elif isinstance(target_root, HTTPFileSystem):
        assert target_root.fs.get_fsspec_remote_protocol() == "http"
    elif isinstance(target_root, S3FileSystem):
        assert target_root.fs.get_fsspec_remote_protocol() == "s3"
    elif isinstance(target_root, GCSFileSystem):
        assert target_root.fs.get_fsspec_remote_protocol() == "gcs"
