import pytest

from pangeo_forge.patterns import ConcatDim, FilePattern, MergeDim, pattern_from_file_sequence


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
        assert fp[key] == format_function(key[0])


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
        assert fp[key] == file_sequence[key[0]]
    assert list(fp.items()) == list(zip(expected_keys, file_sequence))


@pytest.mark.parametrize("pickle", [False, True])
def test_file_pattern_concat_merge(pickle):
    concat = ConcatDim(name="time", keys=list(range(3)))
    merge = MergeDim(name="variable", keys=["foo", "bar"])

    def format_function(time, variable):
        return f"T_{time}_V_{variable}"

    fp = FilePattern(format_function, merge, concat)

    if pickle:
        # regular pickle doesn't work here because it can't pickle format_funciton
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
        assert fp[key] == fname
        fnames.append(fname)
    assert list(fp.items()) == list(zip(expected_keys, fnames))
