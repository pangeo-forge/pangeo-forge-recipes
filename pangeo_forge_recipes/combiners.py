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

    concat_dims: List[Dimension]
    identical_dims: List[Dimension]
    mzz_kwargs: dict = field(default_factory=dict)

    def create_accumulator(self):
        return None

    def add_input(self, accumulator: MultiZarrToZarr, item: dict) -> MultiZarrToZarr:
        if not accumulator:
            references = [item]
        else:
            references = [accumulator.translate(), item]
        return MultiZarrToZarr(
            references,
            concat_dims=self.concat_dims,
            identical_dims=self.identical_dims,
            **self.mzz_kwargs
        )

    def merge_accumulators(self, accumulators: Sequence[MultiZarrToZarr]) -> MultiZarrToZarr:
        references = [a.translate() for a in accumulators]
        return MultiZarrToZarr(
            references,
            concat_dims=self.concat_dims,
            identical_dims=self.identical_dims,
            **self.mzz_kwargs
        )

    def extract_output(self, accumulator: MultiZarrToZarr) -> MultiZarrToZarr:
        return accumulator
