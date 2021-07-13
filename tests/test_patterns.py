import pytest

from pangeo_forge_recipes.patterns import (
    ConcatDim,
    FilePattern,
    MergeDim,
    SubsetDim,
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
    expected_keys = [(0,), (1,), (2,)]
    assert list(fp) == expected_keys
    for key in expected_keys:
        open_spec = fp[key]
        assert open_spec.fname == format_function(key[0])
        assert len(open_spec.subsets) == 0


def test_pattern_from_file_sequence():
    file_sequence = ["T_0", "T_1", "T_2"]
    fp = pattern_from_file_sequence(file_sequence, "time")
    assert fp.dims == {"time": 3}
    assert fp.shape == (3,)
    assert fp.merge_dims == []
    assert fp.concat_dims == ["time"]
    assert fp.nitems_per_input == {"time": None}
    assert fp.concat_sequence_lens == {"time": None}
    expected_keys = [(0,), (1,), (2,)]
    assert list(fp) == expected_keys
    for key in expected_keys:
        assert fp[key].fname == file_sequence[key[0]]


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
    expected_keys = [(0, 0), (0, 1), (0, 2), (1, 0), (1, 1), (1, 2)]
    assert list(fp) == expected_keys
    fnames = []
    for key in expected_keys:
        fname = format_function(variable=merge.keys[key[0]], time=concat.keys[key[1]])
        assert fp[key].fname == fname
        fnames.append(fname)


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


@pytest.mark.parametrize("subset_factor", [1, 2])
def test_subset_single_file(subset_factor):
    def format_function():
        return "fname.nc"

    subset_dim = SubsetDim(dim="time", subset_factor=subset_factor)
    fp = FilePattern(format_function, subset_dim)

    assert fp.dims == {"time_subset": subset_factor}
    assert fp.shape == (subset_factor,)
    assert fp.subset_dims == ["time_subset"]
    expected_keys = [(i,) for i in range(subset_factor)]
    assert list(fp) == expected_keys
    for key in fp:
        open_spec = fp[key]
        assert open_spec.fname == "fname.nc"
        assert len(open_spec.subsets) == 1
        subset_spec = open_spec.subsets[0]
        assert subset_spec.this_segment == key[0]
        assert subset_spec.total_segments == subset_factor
