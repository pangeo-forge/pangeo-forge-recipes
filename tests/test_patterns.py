import pytest

from pangeo_forge.patterns import ExplicitURLSequence, URLPattern, VariableSequencePattern


def test_url_pattern():
    pattern = URLPattern("{foo}:{bar}", {"foo": [1, 2], "bar": ["a", "b", "c"]})
    assert list(pattern) == [
        ((1, "a"), "1:a"),
        ((1, "b"), "1:b"),
        ((1, "c"), "1:c"),
        ((2, "a"), "2:a"),
        ((2, "b"), "2:b"),
        ((2, "c"), "2:c"),
    ]


def test_variable_sequence_pattern():
    pattern = VariableSequencePattern(
        "{variable}:{time}", {"variable": ["temp", "salt"], "time": [1, 2, 3]}
    )
    assert list(pattern) == [
        (("temp", 1), "temp:1"),
        (("temp", 2), "temp:2"),
        (("temp", 3), "temp:3"),
        (("salt", 1), "salt:1"),
        (("salt", 2), "salt:2"),
        (("salt", 3), "salt:3"),
    ]
    assert pattern._sequence_key == "time"

    with pytest.raises(ValueError, match=r".*`variable`.*"):
        _ = VariableSequencePattern("{foo}:{bar}", {"foo": ["temp", "salt"], "time": [1, 2, 3]})

    with pytest.raises(ValueError, match=r".*two keys.*"):
        _ = VariableSequencePattern(
            "{variable}:{time}:{foo}",
            {"variable": ["temp", "salt"], "time": [1, 2, 3], "foo": ["a"]},
        )


def test_explicit_url_sequence():
    seq = ExplicitURLSequence(["a", "b", "c"])
    assert list(seq) == [((0,), "a"), ((1,), "b"), ((2,), "c")]
