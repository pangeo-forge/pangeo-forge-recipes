import operator
from dataclasses import dataclass, field
from functools import reduce
from typing import List, Sequence, Tuple

import apache_beam as beam
from kerchunk.combine import MultiZarrToZarr

from .aggregation import XarrayCombineAccumulator, XarraySchema
from .types import CombineOp, Dimension, Index


@dataclass
class CombineXarraySchemas(beam.CombineFn):
    """A beam ``CombineFn`` which we can use to combine multiple xarray schemas
    along a single dimension

    :param dimension: The dimension along which to combine
    """

    dimension: Dimension

    def get_position(self, index: Index) -> int:
        return index[self.dimension].value

    def create_accumulator(self) -> XarrayCombineAccumulator:
        concat_dim = self.dimension.name if self.dimension.operation == CombineOp.CONCAT else None
        return XarrayCombineAccumulator(concat_dim=concat_dim)

    def add_input(self, accumulator: XarrayCombineAccumulator, item: Tuple[Index, XarraySchema]):
        index, schema = item
        position = self.get_position(index)
        accumulator.add_input(schema, position)
        return accumulator

    def merge_accumulators(
        self, accumulators: Sequence[XarrayCombineAccumulator]
    ) -> XarrayCombineAccumulator:
        if len(accumulators) == 1:
            return accumulators[0]
        # mypy did not like sum(accumulators)
        return reduce(operator.add, accumulators)

    def extract_output(self, accumulator) -> dict:
        return accumulator.schema


@dataclass
class CombineMultiZarrToZarr(beam.CombineFn):
    """A beam ``CombineFn`` for combining Kerchunk ``MultiZarrToZarr`` objects."""

    concat_dims: List[str]
    identical_dims: List[str]
    mzz_kwargs: dict = field(default_factory=dict)

    def to_mzz(self, references):
        return MultiZarrToZarr(
            references,
            concat_dims=self.concat_dims,
            identical_dims=self.identical_dims,
            **self.mzz_kwargs
        )

    def create_accumulator(self):
        return None

    def add_input(self, accumulator: MultiZarrToZarr, item: list[dict]) -> MultiZarrToZarr:
        # in most cases, `item` will be a single-element list containing a single reference.
        # for grib files containing multiple messages, however, `item` will contain > 1 elements.
        # in this case, we need to pre-compile those refs into a single ref. if we do not do this,
        # `merge_accumulators` may hit chunk size mismatch errors.
        item = item if not len(item) > 1 else [self.to_mzz(item).translate()]

        if not accumulator:
            references = item
        else:
            references = [accumulator.translate()] + item
        return self.to_mzz(references)

    def merge_accumulators(self, accumulators: Sequence[MultiZarrToZarr]) -> MultiZarrToZarr:
        references = [a.translate() for a in accumulators]
        return self.to_mzz(references)

    def extract_output(self, accumulator: MultiZarrToZarr) -> MultiZarrToZarr:
        return accumulator
