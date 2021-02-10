"""
Filename patterns.
"""

import itertools
from abc import ABC
from dataclasses import dataclass
from typing import Dict, Generator, Iterable, List, Tuple


class BasePattern(ABC):
    def __iter__(self) -> Generator[Tuple[Tuple, str], None, None]:
        pass


@dataclass
class URLPattern(BasePattern):
    fmt_string: str
    keys: Dict[str, Iterable]

    def __post_init__(self):
        pass
        # TODO: make a better check
        # this check does not work if you have to use the keys multiple times in the format string
        # fmt_string_keys = [
        #     t[1] for t in string.Formatter().parse(self.fmt_string) if t[1] is not None
        # ]
        # e.g. '{variable}/{variable[0]}.nc'
        # if not set(fmt_string_keys) == set(self.keys):
        #    raise KeyError("Specified keys don't match fmt_string")

    def __iter__(self):
        for keys in itertools.product(*self.keys.values()):
            format_kwargs = {k: v for k, v in zip(self.keys, keys)}
            yield keys, self.fmt_string.format(**format_kwargs)


@dataclass
class VariableSequencePattern(URLPattern):
    def __post_init__(self):

        if not len(self.keys) == 2:
            raise ValueError("Exactly two keys are required for VariableSequencePattern")
        if "variable" not in self.keys:
            raise ValueError("keys must contain `variable` for VariableSequencePattern")
        self._sequence_key = [k for k in self.keys if k != "variable"][0]

        super().__post_init__()


@dataclass
class ExplicitURLSequence(BasePattern):
    urls: List[str]

    def __iter__(self):
        for n, item in enumerate(self.urls):
            yield n, item
