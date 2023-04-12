def calc_subsets(sequence_len: int, n_segments: int) -> list[int]:
    """Given a sequence length, return a list that divides it into n_segments
    (possibly uneven) integer segments."""
    if n_segments > sequence_len:
        raise ValueError(f"Can't split len {sequence_len} into {n_segments} segments")
    step = sequence_len // n_segments
    remainder = sequence_len % n_segments
    return (n_segments - 1) * [step] + [step + remainder]
