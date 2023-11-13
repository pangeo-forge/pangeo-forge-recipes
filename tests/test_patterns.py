import inspect

import pytest

from pangeo_forge_recipes.patterns import (
    CombineOp,
    ConcatDim,
    FilePattern,
    FileType,
    MergeDim,
    augment_index_with_start_stop,
    pattern_from_file_sequence,
)
from pangeo_forge_recipes.types import IndexedPosition, Position


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

    fp = FilePattern(format_function, merge, concat, **kwargs)
    return fp, times, varnames, format_function, kwargs


@pytest.fixture
def concat_merge_pattern():
    return make_concat_merge_pattern()


@pytest.fixture(params=[dict(fsspec_open_kwargs={"block_size": "foo"}), dict(file_type="opendap")])
def concat_merge_pattern_with_kwargs(request):
    return make_concat_merge_pattern(**request.param)


@pytest.fixture(
    params=[
        {},
        dict(fsspec_open_kwargs={"username": "foo", "password": "bar"}),
        dict(query_string_secrets={"token": "foo"}),
    ]
)
def runtime_secrets(request):
    return request.param


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
    for index in fp:
        position = next(iter(index.values()))
        assert fp[index] == file_sequence[position.value]


@pytest.mark.parametrize("pickle", [False, True])
def test_file_pattern_concat_merge(runtime_secrets, pickle, concat_merge_pattern_with_kwargs):
    fp, times, varnames, format_function, kwargs = concat_merge_pattern_with_kwargs

    if runtime_secrets:
        if "fsspec_open_kwargs" in runtime_secrets.keys():
            if not fp.file_type == FileType.opendap:
                fp.fsspec_open_kwargs.update(runtime_secrets["fsspec_open_kwargs"])
            else:
                pytest.skip(
                    "`fsspec_open_kwargs` should never be used in combination with `opendap`."
                    " This is checked in `FilePattern.__init__` but not when updating attributes. "
                    "Proposed changes to secret handling will obviate the need for runtime updates"
                    " to attributes in favor of encryption. So for now, we'll just skip this."
                )
        if "query_string_secrets" in runtime_secrets.keys():
            fp.query_string_secrets.update(runtime_secrets["query_string_secrets"])

    if pickle:
        # regular pickle doesn't work here because it can't pickle format_function
        from cloudpickle import dumps, loads

        fp = loads(dumps(fp))

    assert fp.dims == {"variable": 2, "time": 3}
    assert fp.shape == (
        2,
        3,
    )
    assert fp.merge_dims == ["variable"]
    assert fp.concat_dims == ["time"]
    assert fp.nitems_per_input == {"time": None}
    assert fp.concat_sequence_lens == {"time": None}
    assert len(list(fp)) == 6
    for index in fp:
        concat_dim_key = index.find_concat_dim("time")
        assert index.find_concat_dim("foobar") is None
        for dimension, position in index.items():
            if dimension.name == "time":
                assert dimension.operation == CombineOp.CONCAT
                time_val = times[position.value]
                assert position == index[concat_dim_key]
            if dimension.name == "variable":
                assert dimension.operation == CombineOp.MERGE
                variable_val = varnames[position.value]
        expected_fname = format_function(time=time_val, variable=variable_val)
        assert fp[index] == expected_fname

    if "fsspec_open_kwargs" in kwargs.keys():
        assert fp.file_type != FileType.opendap
        if "fsspec_open_kwargs" in runtime_secrets.keys():
            kwargs["fsspec_open_kwargs"].update(runtime_secrets["fsspec_open_kwargs"])
        assert fp.fsspec_open_kwargs == kwargs["fsspec_open_kwargs"]
    if "query_string_secrets" in runtime_secrets.keys():
        assert fp.query_string_secrets == runtime_secrets["query_string_secrets"]
    if kwargs.get("file_type", None) == "opendap":
        assert fp.file_type == FileType.opendap
        assert fp.fsspec_open_kwargs == {}


def test_incompatible_kwargs():
    kwargs = dict(fsspec_open_kwargs={"block_size": "foo"}, file_type="opendap")
    with pytest.raises(ValueError):
        make_concat_merge_pattern(**kwargs)
        return


@pytest.mark.parametrize("nkeep", [1, 2])
def test_prune(nkeep, concat_merge_pattern_with_kwargs, runtime_secrets):
    fp = concat_merge_pattern_with_kwargs[0]

    if runtime_secrets:
        if "fsspec_open_kwargs" in runtime_secrets.keys():
            if fp.file_type != FileType.opendap:
                fp.fsspec_open_kwargs.update(runtime_secrets["fsspec_open_kwargs"])
            else:
                pytest.skip(
                    "`fsspec_open_kwargs` should never be used in combination with `opendap`."
                    " This is checked in `FilePattern.__init__` but not when updating attributes. "
                    "Proposed changes to secret handling will obviate the need for runtime updates"
                    " to attributes in favor of encryption. So for now, we'll just skip this."
                )
        if "query_string_secrets" in runtime_secrets.keys():
            fp.query_string_secrets.update(runtime_secrets["query_string_secrets"])

    fp_pruned = fp.prune(nkeep=nkeep)
    assert fp_pruned.dims == {"variable": 2, "time": nkeep}
    assert len(list(fp_pruned.items())) == 2 * nkeep

    def get_kwargs(file_pattern):
        sig = inspect.signature(file_pattern.__init__)
        kwargs = {
            param: getattr(file_pattern, param)
            for param in sig.parameters.keys()
            if param not in ["combine_dims"]
        }
        return kwargs

    assert get_kwargs(fp) == get_kwargs(fp_pruned)


@pytest.mark.parametrize("file_type_value", [ft.value for ft in list(FileType)] + ["unsupported"])
def test_setting_file_types(file_type_value):
    file_type_kwargs = {"file_type": file_type_value}

    if not file_type_value == "unsupported":
        fp = make_concat_merge_pattern(**file_type_kwargs)[0]
        assert fp.file_type == FileType(file_type_value)
    else:
        with pytest.raises(ValueError, match=rf"'{file_type_value}' is not a valid FileType"):
            fp = make_concat_merge_pattern(**file_type_kwargs)[0]


@pytest.mark.parametrize(
    "position,start",
    [(0, 0), (1, 2), (2, 4), (3, 7), (4, 9)],
)
def test_augment_index_with_start_stop(position, start):
    dk = Position(position)
    expected = IndexedPosition(start, dimsize=11)
    actual = augment_index_with_start_stop(dk, [2, 2, 3, 2, 2])
    assert actual == expected
