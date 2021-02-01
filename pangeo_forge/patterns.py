"""
Filename patterns.
"""

import itertools
import string
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
        fmt_string_keys = [
            t[1] for t in string.Formatter().parse(self.fmt_string) if t[1] is not None
        ]
        if not set(fmt_string_keys) == set(self.keys):
            raise KeyError("Specified keys don't match fmt_string")

    def __iter__(self):
        for keys in itertools.product(*self.keys.values()):
            format_kwargs = {k: v for k, v in zip(self.keys, keys)}
            yield keys, self.fmt_string.format(**format_kwargs)


@dataclass
class ExplicitURLSequence(BasePattern):
    urls: List[str]

    def __iter__(self):
        for n, item in enumerate(self.urls):
            yield n, item
