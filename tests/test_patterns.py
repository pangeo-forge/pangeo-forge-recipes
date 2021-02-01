from pangeo_forge.patterns import ExplicitURLSequence, URLPattern


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


def test_explicit_url_sequence():
    seq = ExplicitURLSequence(["a", "b", "c"])
    assert list(seq) == [(0, "a"), (1, "b"), (2, "c")]
