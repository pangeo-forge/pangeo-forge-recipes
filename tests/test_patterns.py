import pytest

from pangeo_forge_recipes.patterns import (
    CombineOp,
    ConcatDim,
    FilePattern,
    MergeDim,
    pattern_from_file_sequence,
    prune_pattern,
)


def test_file_pattern_concat():
    concat = ConcatDim(name="time", keys=list(range(3)))

    def format_function(time):
        return f"T_{time}"

    fp = FilePattern(format_function, concat)
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


@pytest.mark.parametrize("pickle", [False, True])
def test_file_pattern_concat_merge(pickle):
    concat = ConcatDim(name="time", keys=list(range(3)))
    merge = MergeDim(name="variable", keys=["foo", "bar"])

    def format_function(time, variable):
        return f"T_{time}_V_{variable}"

    fp = FilePattern(format_function, merge, concat)

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
        fname = format_function(**{k.name: k.index for k in key})
        for k in key:
            if k.name == "time":
                assert k.operation == CombineOp.CONCAT
            if k.name == "variable":
                assert k.operation == CombineOp.MERGE
        assert fp[key] == fname


@pytest.mark.parametrize("nkeep", [1, 2])
def test_prune(nkeep):
    concat = ConcatDim(name="time", keys=list(range(3)))
    merge = MergeDim(name="variable", keys=["foo", "bar"])

    def format_function(time, variable):
        return f"T_{time}_V_{variable}"

    fp = FilePattern(format_function, merge, concat)
    fp_pruned = prune_pattern(fp, nkeep=nkeep)
    assert fp_pruned.dims == {"variable": 2, "time": nkeep}
    assert len(list(fp_pruned.items())) == 2 * nkeep
