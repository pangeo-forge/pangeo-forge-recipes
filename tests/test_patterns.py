import pytest

from pangeo_forge_recipes.patterns import (
    CombineOp,
    ConcatDim,
    FilePattern,
    MergeDim,
    pattern_from_file_sequence,
    prune_pattern,
)


@pytest.fixture
def concat_pattern():
    concat = ConcatDim(name="time", keys=list(range(3)))

    def format_function(time):
        return f"T_{time}"

    return FilePattern(format_function, concat)


def make_concat_merge_pattern(**kwargs):
    times = list(range(3))
    varnames = ["foo", "bar"]
    concat = ConcatDim(name="time", keys=times)
    merge = MergeDim(name="variable", keys=varnames)

    def format_function(time, variable):
        return f"T_{time}_V_{variable}"

    if "fsspec_open_kwargs" in kwargs.keys() and "is_opendap" in kwargs.keys():
        with pytest.raises(ValueError):
            fp = FilePattern(format_function, merge, concat, **kwargs)
            return
    else:
        fp = FilePattern(format_function, merge, concat, **kwargs)
        return fp, times, varnames, format_function, kwargs


@pytest.fixture
def concat_merge_pattern():
    return make_concat_merge_pattern()


@pytest.fixture(
    params=[
        dict(fsspec_open_kwargs={"block_size": "foo"}),
        dict(is_opendap=True),
        dict(fsspec_open_kwargs={"block_size": "foo"}, is_opendap=True),
    ]
)
def concat_merge_pattern_with_kwargs(request):
    return make_concat_merge_pattern(**request.param)


def test_file_pattern_concat(concat_pattern):
    fp = concat_pattern
    assert fp.dims == {"time": 3}
    assert fp.shape == (3,)
    assert fp.merge_dims == []
    assert fp.concat_dims == ["time"]
    assert fp.nitems_per_input == {"time": None}
    assert fp.concat_sequence_lens == {"time": None}
    assert len(list(fp)) == 3
    for key, expected_value in zip(fp, ["T_0", "T_1", "T_2"]):
        assert fp[key] == expected_value


def test_pattern_from_file_sequence():
    file_sequence = ["T_0", "T_1", "T_2"]
    fp = pattern_from_file_sequence(file_sequence, "time")
    assert fp.dims == {"time": 3}
    assert fp.shape == (3,)
    assert fp.merge_dims == []
    assert fp.concat_dims == ["time"]
    assert fp.nitems_per_input == {"time": None}
    assert fp.concat_sequence_lens == {"time": None}
    for key in fp:
        assert fp[key] == file_sequence[key[0].index]


@pytest.mark.parametrize(
    "runtime_secrets",
    [
        {},
        dict(fsspec_open_kwargs={"username": "foo", "password": "bar"}),
        dict(query_string_secrets={"token": "foo"}),
    ],
)
@pytest.mark.parametrize("pickle", [False, True])
def test_file_pattern_concat_merge(runtime_secrets, pickle, concat_merge_pattern_with_kwargs):
    if not concat_merge_pattern_with_kwargs:
        # if `fsspec_open_kwargs` are passed with `is_opendap`, `FilePattern.__init__` raises
        # ValueError and `concat_merge_pattern_with_kwargs` returns None, so nothing to test
        return
    else:
        fp, times, varnames, format_function, kwargs = concat_merge_pattern_with_kwargs

    if runtime_secrets:
        if "fsspec_open_kwargs" in runtime_secrets.keys():
            if not fp.is_opendap:
                fp.fsspec_open_kwargs.update(runtime_secrets["fsspec_open_kwargs"])
            else:
                return
        if "query_string_secrets" in runtime_secrets.keys():
            fp.query_string_secrets.update(runtime_secrets["query_string_secrets"])

    if pickle:
        # regular pickle doesn't work here because it can't pickle format_function
        from cloudpickle import dumps, loads

        fp = loads(dumps(fp))

    assert fp.dims == {"variable": 2, "time": 3}
    assert fp.shape == (2, 3,)
    assert fp.merge_dims == ["variable"]
    assert fp.concat_dims == ["time"]
    assert fp.nitems_per_input == {"time": None}
    assert fp.concat_sequence_lens == {"time": None}
    assert len(list(fp)) == 6
    for key in fp:
        expected_fname = format_function(time=times[key[1].index], variable=varnames[key[0].index])
        for k in key:
            if k.name == "time":
                assert k.operation == CombineOp.CONCAT
                assert k.sequence_len == 3
            if k.name == "variable":
                assert k.operation == CombineOp.MERGE
                assert k.sequence_len == 2
        assert fp[key] == expected_fname
        # make sure key order doesn't matter
        assert fp[key[::-1]] == expected_fname

    if "fsspec_open_kwargs" in kwargs.keys():
        assert fp.is_opendap is False
        if "fsspec_open_kwargs" in runtime_secrets.keys():
            kwargs["fsspec_open_kwargs"].update(runtime_secrets["fsspec_open_kwargs"])
        assert fp.fsspec_open_kwargs == kwargs["fsspec_open_kwargs"]
    if "query_string_secrets" in runtime_secrets.keys():
        assert fp.query_string_secrets == runtime_secrets["query_string_secrets"]
    if "is_opendap" in kwargs.keys():
        assert fp.is_opendap == kwargs["is_opendap"]
        assert fp.is_opendap is True
        assert fp.fsspec_open_kwargs == {}


@pytest.mark.parametrize("nkeep", [1, 2])
def test_prune(nkeep, concat_merge_pattern):
    fp = concat_merge_pattern[0]
    fp_pruned = prune_pattern(fp, nkeep=nkeep)
    assert fp_pruned.dims == {"variable": 2, "time": nkeep}
    assert len(list(fp_pruned.items())) == 2 * nkeep
